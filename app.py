import os
import json
import logging
import time
import requests
import stripe
import uuid
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from flask_cors import CORS
from redis import Redis
from apscheduler.schedulers.background import BackgroundScheduler
from werkzeug.middleware.proxy_fix import ProxyFix
from typing import List, Dict, Any

from odds_api import fetch_player_props, parse_game_data, enrich_player_props
from enrichment import load_props_from_file
from probability import implied_probability, calculate_edge, kelly_bet_size, calculate_parlay_edge
from prop_deduplication import deduplicate_props_by_player, get_stat_display_name, get_player_avatar_url
from pairing import build_props_novig
from trends_l10 import annotate_props_with_l10  # NEW

from team_abbreviations import get_team_abbreviation, format_matchup, TEAM_ABBREVIATIONS

# --- ADD near top of app.py
import os, requests
from datetime import datetime, timedelta, timezone

# Try to use your team abbreviations if present; otherwise fallback to full names
try:
    from team_abbreviations import TEAM_ABBR as _TEAM_ABBR
    def _abbr(team):
        return _TEAM_ABBR.get(team, team)
except Exception:
    def _abbr(team):  # fallback
        return team

SPORT_KEYS = {
    "mlb": "baseball_mlb",
    "nfl": "americanfootball_nfl",
    "nba": "basketball_nba",
    "nhl": "icehockey_nhl",
}

# Use Odds API market keys (left) -> internal stat keys (right)
MLB_PROP_MARKETS = {
    "batter_hits": "batter_hits",
    "batter_home_runs": "batter_home_runs",
    "batter_total_bases": "batter_total_bases",
    "pitcher_strikeouts": "pitcher_strikeouts",
    "batter_rbis": "rbis",
    "batter_runs_scored": "runs",
    # (Optional extras as you roll them in)
    # "batter_hits_runs_rbis": "hrr",
    # "batter_walks": "batter_walks",
    # "batter_stolen_bases": "stolen_bases",
}

def _date_range_utc(date_iso: str | None):
    if not date_iso:
        return None, None
    # Interpret date as local (America/Phoenix) midnight-to-midnight, then to UTC
    # Phoenix is UTC-7 year-round (no DST)
    try:
        y, m, d = [int(x) for x in date_iso.split("-")]
        start_local = datetime(y, m, d, 0, 0, 0)
        end_local   = start_local + timedelta(days=1)
        # Convert to UTC by adding 7 hours (Phoenix UTC-7)
        start_utc = (start_local + timedelta(hours=7)).replace(tzinfo=timezone.utc)
        end_utc   = (end_local + timedelta(hours=7)).replace(tzinfo=timezone.utc)
        return start_utc.isoformat().replace("+00:00", "Z"), end_utc.isoformat().replace("+00:00", "Z")
    except Exception:
        return None, None

def _abbr(team: str):
    try:
        from team_abbreviations import TEAM_ABBR
        return TEAM_ABBR.get(team, team)
    except Exception:
        return team

def mk_matchup(away_team: str, home_team: str) -> str:
    a = (_abbr(away_team) or "").strip().replace(" ", "")
    h = (_abbr(home_team) or "").strip().replace(" ", "")
    return f"{a}@{h}"

def fetch_player_prop_offers_flat(league: str = "mlb",
                                  date_iso: str | None = None,
                                  books: list[str] | None = None,
                                  markets: list[str] | None = None) -> list[dict]:
    """
    Return flat offers with explicit side+book so we can de-vig:
      { event_key, matchup, league, player, stat, line, side, odds, book }
    """
    ODDS_API_KEY = os.getenv("ODDS_API_KEY")
    if not ODDS_API_KEY:
        raise RuntimeError("ODDS_API_KEY is not set")

    sport_key = SPORT_KEYS.get(league.lower())
    if not sport_key:
        raise ValueError(f"Unsupported league: {league}")

    # Default markets/books
    if league.lower() == "mlb":
        valid_markets = list(MLB_PROP_MARKETS.keys())
    else:
        valid_markets = markets or []  # fill later for NFL/NBA/NHL

    if not books:
        books = [b.strip().lower() for b in os.getenv("BOOKS", "draftkings,fanduel,betmgm").split(",") if b.strip()]

    # 1) List events for the window (free; no quota cost)
    ev_params = {"apiKey": ODDS_API_KEY}
    start_utc, end_utc = _date_range_utc(date_iso)
    if start_utc and end_utc:
        ev_params["commenceTimeFrom"] = start_utc
        ev_params["commenceTimeTo"] = end_utc

    ev_url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
    ev = requests.get(ev_url, params=ev_params, timeout=20)
    ev.raise_for_status()
    events = ev.json() or []

    # Build quick lookup for matchup
    try:
        from team_abbreviations import TEAM_ABBREVIATIONS as TEAM_ABBR
    except Exception:
        TEAM_ABBR = None

    out: list[dict] = []
    if not events:
        return out  # nothing to fetch today

    # 2) For each event, call the **event odds** endpoint with prop markets
    for e in events:
        event_id = e.get("id")
        if not event_id:
            continue
        away = e.get("away_team") or ""
        home = e.get("home_team") or ""
        matchup = mk_matchup(away, home)

        eo_params = {
            "apiKey": ODDS_API_KEY,
            "regions": "us",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "bookmakers": ",".join(books),
            "markets": ",".join(valid_markets),
        }
        eo_url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
        try:
            resp = requests.get(eo_url, params=eo_params, timeout=20)
            resp.raise_for_status()
        except requests.HTTPError as http_err:
            # 404 or no markets? skip silently; 422 shouldn't happen here
            continue

        data = resp.json() or {}
        for bm in (data.get("bookmakers") or []):
            book_key = (bm.get("key") or bm.get("title") or "").lower().replace(" ", "_")
            if book_key not in books:
                continue
            for mk in (bm.get("markets") or []):
                mkey = mk.get("key")
                internal_stat = MLB_PROP_MARKETS.get(mkey)
                if not internal_stat:
                    continue
                for oc in (mk.get("outcomes") or []):
                    side = (oc.get("name") or "").lower()  # "over" | "under" expected
                    if side not in ("over", "under"):
                        continue
                    player = oc.get("description") or oc.get("participant") or oc.get("player") or ""
                    point = oc.get("point")
                    price = oc.get("price")
                    if not player or point is None or price is None:
                        continue
                    try:
                        out.append({
                            "event_key": str(event_id),
                            "matchup": matchup,
                            "league": league.lower(),
                            "player": player,
                            "stat": internal_stat,
                            "line": float(point),
                            "side": side,
                            "odds": int(price),
                            "book": book_key,
                        })
                    except Exception:
                        continue

    return out

# NFL modules
from nfl_odds_api import fetch_nfl_props
from nfl_enrichment import enrich_nfl_props
from nfl_contextual import add_nfl_context

# MLB game context enrichment
from mlb_game_enrichment import enrich_mlb_props_with_context, filter_positive_environment_props

# Configure logging - reduce external API noise
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Disable debug logging for external APIs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "mora-bets-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

# --- Boot logging with git info ---
def _git_info():
    try:
        commit = subprocess.check_output(["git","rev-parse","--short","HEAD"]).decode().strip()
        branch = subprocess.check_output(["git","rev-parse","--abbrev-ref","HEAD"]).decode().strip()
    except Exception:
        commit = branch = "unknown"
    return branch, commit

try:
    b, c = _git_info()
    logger.info(f"🚀 Booting MoraBets app @ {b}:{c}")
except Exception:
    pass

# Stripe configuration
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')
LICENSE_DB = 'license_keys.json'

# Updated Stripe configuration for monthly/yearly pricing
PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")
PRICE_MONTHLY = os.environ.get("STRIPE_PRICE_ID_MONTHLY", "price_1RtyVnIzLEeC8QTzhOrtq2CO")
PRICE_YEARLY = os.environ.get("STRIPE_PRICE_ID_YEARLY", "price_1RtyYYIzLEeC8QTzw8JsGH39")
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "3"))
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:5000")

# Legacy price lookup for backward compatibility
PRICE_LOOKUP = {
    'prod_SjjH7D6kkxRbJf': 'price_1RoFpPIzLEeC8QTz5kdeiLyf',  # Calculator Tool - $9.99/month
    'prod_Sjkk8GQGPBvuOP': 'price_1RoHFOIzLEeC8QTziT9k1t45'   # Mora Assist - $28.99
}



# Redis configuration with robust stability features
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis = None
memory_cache = {}  # In-memory fallback cache
redis_healthy = False
redis_last_check = 0

def init_redis():
    """Initialize Redis connection with proper ping validation"""
    global redis, redis_healthy
    
    try:
        redis = Redis.from_url(redis_url)
        redis.ping()  # confirms active connection
        redis_healthy = True
        print("✅ Connected to Redis successfully")
        logger.info(f"✅ Connected to Redis at {redis_url}")
        return True
    except Exception as e:
        print("⚠️ Redis connection failed, using in-memory cache:", e)
        logger.warning(f"❌ Failed to connect to Redis URL {redis_url}: {e}")
        try:
            # Fallback to local Redis
            redis = Redis(host='localhost', port=6379, db=0)
            redis.ping()
            redis_healthy = True
            print("✅ Connected to local Redis successfully")
            logger.info("✅ Connected to local Redis at localhost:6379")
            return True
        except Exception as e2:
            print("⚠️ Local Redis connection failed, using in-memory cache:", e2)
            logger.warning(f"❌ Failed to connect to local Redis: {e2}")
            logger.info("🔄 Using in-memory cache as fallback")
            redis = None  # fallback flag
            redis_healthy = False
            return False

def check_redis_health():
    """Check Redis health and attempt reconnection if needed"""
    global redis_healthy, redis_last_check
    import time
    
    current_time = time.time()
    # Check every 30 seconds
    if current_time - redis_last_check < 30:
        return redis_healthy
    
    redis_last_check = current_time
    
    if redis:
        try:
            redis.ping()
            if not redis_healthy:
                logger.info("✅ Redis connection restored")
            redis_healthy = True
            return True
        except Exception as e:
            if redis_healthy:
                logger.warning(f"❌ Redis connection lost: {e}")
            redis_healthy = False
            # Attempt reconnection
            logger.info("🔄 Attempting Redis reconnection...")
            return init_redis()
    else:
        # No Redis connection, try to establish one
        logger.info("🔄 Attempting initial Redis connection...")
        return init_redis()

# Initialize Redis on startup
init_redis()

# No-Vig Mode Configuration
USE_NOVIG_ONLY = os.getenv("ENABLE_ENRICHMENT", "false").lower() != "true"
DEFAULT_BOOKS = [b.strip() for b in os.getenv("BOOKS", "draftkings,fanduel,betmgm").split(",") if b.strip()]

# Cache helper functions with enhanced stability and timeouts
def cache_set(key, value, timeout=3):
    """Set cache value with Redis or memory fallback - non-blocking"""
    # Check Redis health periodically
    check_redis_health()
    
    if redis and redis_healthy:
        try:
            # Use pipeline for better performance and atomicity
            pipe = redis.pipeline()
            pipe.set(key, value)
            pipe.execute()
            return True
        except Exception as e:
            logger.warning(f"Redis set failed for key {key}: {e}")
            # Fall back to memory cache
            memory_cache[key] = value
            return False
    else:
        # Always store in memory cache as fallback
        memory_cache[key] = value
        return False

def cache_get(key, timeout=3):
    """Get cache value with Redis or memory fallback - non-blocking"""
    # Check Redis health periodically
    check_redis_health()
    
    if redis and redis_healthy:
        try:
            # Try Redis first
            value = redis.get(key)
            if value is not None:
                return value
            # If not in Redis, check memory cache
            return memory_cache.get(key)
        except Exception as e:
            logger.warning(f"Redis get failed for key {key}: {e}")
            # Fall back to memory cache
            return memory_cache.get(key)
    else:
        # Use memory cache only
        return memory_cache.get(key)

def cache_incr(key, timeout=3):
    """Increment cache value with Redis or memory fallback - non-blocking"""
    # Check Redis health periodically
    check_redis_health()
    
    if redis and redis_healthy:
        try:
            result = redis.incr(key)
            # Also update memory cache for consistency
            memory_cache[key] = result
            return result
        except Exception as e:
            logger.warning(f"Redis incr failed for key {key}: {e}")
            # Fall back to memory cache
            memory_cache[key] = memory_cache.get(key, 0) + 1
            return memory_cache[key]
    else:
        # Use memory cache only
        memory_cache[key] = memory_cache.get(key, 0) + 1
        return memory_cache[key]

def cache_exists(key, timeout=3):
    """Check if cache key exists - non-blocking"""
    # Check Redis health periodically
    check_redis_health()
    
    if redis and redis_healthy:
        try:
            return redis.exists(key) or key in memory_cache
        except Exception as e:
            logger.warning(f"Redis exists failed for key {key}: {e}")
            return key in memory_cache
    else:
        return key in memory_cache

@app.route("/")
def home():
    """Redirect to how-it-works landing page"""
    return redirect(url_for("how_it_works"))

@app.route("/how-it-works")
def how_it_works():
    """Landing page explaining how Mora Bets works"""
    return render_template("how_it_works.html")

@app.route("/paywall")
def paywall():
    """Pricing page with Stripe checkout options"""
    return render_template("index.html")

@app.route("/config", methods=["GET"])
def paywall_config():
    """Return paywall configuration for frontend"""
    return jsonify({
        "publicKey": PUBLISHABLE_KEY,
        "priceMonthly": PRICE_MONTHLY,
        "priceYearly": PRICE_YEARLY,
        "trialDays": TRIAL_DAYS
    })

@app.route("/tool")
def tool():
    """Tool access page - requires valid license"""
    # Check if user has valid license in session
    if session.get("licensed"):
        return redirect(url_for("dashboard"))
    else:
        return redirect(url_for("paywall") + "?message=You need a valid license key to access the tool.")

@app.route("/create-checkout-session", methods=['POST'])
def create_checkout_session():
    """Create Stripe checkout session - supports both legacy and new pricing"""
    try:
        # Handle legacy form-based product ID
        product_id = request.form.get('product_id')
        
        # Handle new JSON-based price ID for monthly/yearly toggle
        data = None
        if not product_id:
            try:
                data = request.get_json(force=True)
                price_id = data.get("price_id") if data else None
            except:
                return jsonify({"error": "Missing product or price ID"}), 400
        else:
            price_id = PRICE_LOOKUP.get(product_id)
        
        if not price_id:
            return jsonify({"error": "Invalid product"}), 400
        
        # Validate that price_id is one of our accepted prices
        if price_id not in [PRICE_MONTHLY, PRICE_YEARLY] + list(PRICE_LOOKUP.values()):
            return jsonify({"error": "Invalid price"}), 400
            
        # Configure session
        subscription_data = {}
        
        # Add trial only for monthly subscription
        if price_id == PRICE_MONTHLY and TRIAL_DAYS > 0:
            subscription_data["trial_period_days"] = TRIAL_DAYS
        
        # Legacy trial for old price
        if price_id == 'price_1RoFpPIzLEeC8QTz5kdeiLyf':
            subscription_data["trial_period_days"] = 3
            
        session_config = {
            'line_items': [{'price': price_id, 'quantity': 1}],
            'mode': 'subscription',
            'allow_promotion_codes': True,
            'success_url': f'{APP_BASE_URL}/dashboard?session_id={{CHECKOUT_SESSION_ID}}',
            'cancel_url': f'{APP_BASE_URL}/paywall?canceled=true',
        }
        
        if subscription_data:
            session_config['subscription_data'] = subscription_data
        
        # Enable phone collection and disclaimer for legacy Mora Assist
        if product_id == 'prod_Sjkk8GQGPBvuOP':
            session_config['phone_number_collection'] = {'enabled': True}
            session_config['custom_fields'] = [
                {
                    'key': 'disclaimer',
                    'label': {
                        'type': 'custom',
                        'custom': 'Risk Acknowledgment (18+)'
                    },
                    'type': 'dropdown',
                    'dropdown': {
                        'options': [
                            {'label': 'I agree (not financial advice)', 'value': 'agree'}
                        ]
                    },
                    'optional': False
                }
            ]
        
        session = stripe.checkout.Session.create(**session_config)
        
        # Return JSON response for new API or redirect for legacy
        if data:
            return jsonify({"id": session.id, "url": session.url})
        else:
            return redirect(session.url or request.url_root, code=303)
            
    except Exception as e:
        logger.error(f"Stripe checkout error: {e}")
        logger.error(f"Full traceback: {e}", exc_info=True)
        if data:
            return jsonify({"error": str(e)}), 400
        else:
            return f"Checkout failed: {str(e)}", 500

@app.route("/dashboard")
def dashboard():
    """Main Mora Bets dashboard - protected route"""
    # Check for key parameter
    user_key = request.args.get('key', '').strip()
    
    if user_key:
        # Validate key
        try:
            with open(LICENSE_DB, 'r') as f:
                keys = json.load(f)
        except Exception as e:
            logger.error(f"Error loading license keys: {e}")
            return redirect(url_for('index') + '?message=System+error.+Please+try+again.')
        
        # Check if key exists and is valid (case-insensitive)
        is_valid = False
        for key in keys:
            if key.upper() == user_key.upper() and keys[key]:
                is_valid = True
                break
        
        if not is_valid:
            logger.info(f"Invalid key attempt: {user_key}")
            return redirect(url_for('index') + '?message=Invalid+key.+Please+try+again.')
        
        # Key is valid, set session and render dashboard
        session["licensed"] = True
        session["license_key"] = user_key
        logger.info(f"✅ Dashboard access granted for key: {user_key}")
    
    try:
        hits = cache_incr("hits")
        return render_template("dashboard.html", hits=hits)
    except Exception as e:
        logger.error(f"Error in dashboard route: {e}")
        return f'''
        <!DOCTYPE html>
        <html>
        <head><title>Mora Bets</title></head>
        <body>
        <h1>Mora Bets - Sports Betting Analytics</h1>
        <p>System Status: Running</p>
        <p>Error: {str(e)}</p>
        <p><a href="/health">Health Check</a></p>
        <p><a href="/api/status">API Status</a></p>
        </body>
        </html>
        '''

@app.route("/verify")
def verify():
    """Handle Stripe success and generate license key"""
    session_id = request.args.get('session_id')
    key = request.args.get('key')  # For direct key display
    
    if key:
        return render_template('verify.html', key=key)
    
    if not session_id:
        return render_template('verify.html', error='Missing session ID.')

    try:
        session = stripe.checkout.Session.retrieve(session_id, expand=['customer'])
        if not session.customer_details:
            return render_template('verify.html', error="No customer details found")
        customer_email = session.customer_details.email or "unknown@example.com"
        customer_name = session.customer_details.name or 'user'
        last = customer_name.split()[-1].lower()
        suffix = str(uuid.uuid4().int)[-4:]
        key = f'{last}{suffix}'

        # Load existing keys
        try:
            with open(LICENSE_DB, 'r') as f:
                keys = json.load(f)
        except:
            keys = {}

        # Check if this is Mora Assist (no license key needed)
        line_items = session.get('line_items', {}).get('data', [])
        is_mora_assist = False
        if line_items:
            price_id = line_items[0].get('price', {}).get('id', '')
            is_mora_assist = price_id == 'price_1RoHFOIzLEeC8QTziT9k1t45'
        
        if is_mora_assist:
            # Mora Assist - no license key, just confirmation
            phone_number = getattr(session.customer_details, 'phone', 'Not provided')
            logger.info(f"✅ Mora Assist purchase confirmed: {customer_email}, Phone: {phone_number}")
            return render_template('verify.html', mora_assist=True, email=customer_email, phone=phone_number)
        else:
            # Calculator Tool - generate license key
            keys[key] = {'email': customer_email, 'plan': session.mode}
            with open(LICENSE_DB, 'w') as f:
                json.dump(keys, f)

            logger.info(f"✅ Generated license key for {customer_email}: {key}")
            return render_template('verify.html', key=key)
        
    except Exception as e:
        logger.error(f"❌ Stripe verification error: {e}")
        return render_template('verify.html', error='Verification failed. Please contact support.')

@app.route("/verify-key")
def verify_key():
    """Verify license key for dashboard access"""
    user_key = request.args.get('key', '').strip()
    
    # Load keys from JSON file
    try:
        with open(LICENSE_DB, 'r') as f:
            keys = json.load(f)
    except Exception as e:
        logger.error(f"Error loading license keys: {e}")
        return jsonify({'valid': False})
    
    # Check if key exists and is valid (case-insensitive)
    is_valid = False
    for key in keys:
        if key.upper() == user_key.upper() and keys[key]:
            is_valid = True
            break
    
    logger.info(f"Key verification for '{user_key}': {'Valid' if is_valid else 'Invalid'}")
    
    return jsonify({'valid': is_valid})

@app.route("/validate-key", methods=['POST'])
def validate_key():
    """Validate license key and grant access"""
    user_key = request.form.get('key', '').strip().lower()
    
    # Check master key first
    if user_key == 'mora-king':
        session["licensed"] = True
        session["license_key"] = user_key
        session["access_level"] = "creator"
        logger.info("✅ Master key access granted")
        return jsonify({'valid': True, 'redirect': url_for('dashboard')})
    
    # Check license database
    try:
        with open(LICENSE_DB, 'r') as f:
            keys = json.load(f)
    except:
        return jsonify({'valid': False})
    
    if user_key in keys:
        session["licensed"] = True
        session["license_key"] = user_key
        session["access_level"] = "premium"
        logger.info(f"✅ License key validated: {user_key}")
        return jsonify({'valid': True, 'redirect': url_for('dashboard')})
    
    return jsonify({'valid': False})

@app.before_request
def require_license():
    """Protect dashboard routes except public pages and API endpoints"""
    # Allow access to public pages, verification, health checks, API endpoints, and static files
    public_endpoints = [
        "home", "how_it_works", "paywall", "paywall_config", "tool", "verify", "verify_key", "validate_key", "create_checkout_session", 
        "billing_portal", "health", "ping", "static", "api_status", "get_props", "filtered_moneylines", 
        "logout", "dashboard", "analytics"
    ]
    
    # Also allow access to any route starting with /api/
    if request.endpoint in public_endpoints or request.path.startswith("/static") or request.path.startswith("/api/"):
        return
    
    # Check if user has valid license in session for protected routes
    if not session.get("licensed"):
        return redirect(url_for("paywall"))

@app.route("/health")
def health():
    """Health check endpoint - instant response"""
    return jsonify({"health": "live"}), 200

@app.route("/status")
def status():
    """Simple status endpoint for health checks"""
    return jsonify({"status": "OK"}), 200

@app.route("/billing-portal")
def billing_portal():
    """Create Stripe billing portal session for subscription management"""
    try:
        # This is a placeholder - in production, you'd retrieve the customer ID from your session/database
        # For now, return to paywall with message about contacting support
        return redirect(url_for("paywall") + "?message=To manage your subscription, please contact support with your license key.")
        
        # Future implementation when customer IDs are stored:
        # customer_id = session.get('stripe_customer_id')
        # if not customer_id:
        #     return redirect(url_for("paywall") + "?message=No active subscription found.")
        # 
        # portal_session = stripe.billing_portal.Session.create(
        #     customer=customer_id,
        #     return_url=f'{request.url_root}dashboard'
        # )
        # return redirect(portal_session.url)
    except Exception as e:
        logger.error(f"Billing portal error: {e}")
        return redirect(url_for("paywall") + "?message=Unable to access billing portal. Please contact support.")

@app.route("/logout")
def logout():
    """Clear license session for testing"""
    session.clear()
    return redirect(url_for("how_it_works"))

# Removed extract_team_abbreviation function - now using team_abbreviations.py module

def group_props_by_matchup(props_data):
    """Group player props by actual team matchups using real MLB data"""
    try:
        from team_abbreviations import TEAM_ABBREVIATIONS
        from enrichment import get_player_team_mapping
        
        # Load current games/odds data to get real matchups
        games_data = cache_get("mlb_odds")
        real_matchups = []
        team_to_matchup = {}
        
        if games_data:
            # Handle bytes, string, or dict data types
            if isinstance(games_data, bytes):
                games = json.loads(games_data.decode('utf-8'))
            elif isinstance(games_data, str):
                games = json.loads(games_data)
            else:
                games = games_data
            
            # Build matchup mapping from real game data
            if isinstance(games, list):
                for game in games:
                    if isinstance(game, dict):
                        home_team = game.get("home_team", "")
                        away_team = game.get("away_team", "")
                        
                        if home_team and away_team:
                            # Create matchup key using team abbreviations
                            matchup_key = format_matchup(away_team, home_team)
                            real_matchups.append({
                                "matchup": matchup_key,
                                "home_team": home_team,
                                "away_team": away_team,
                                "home_abbr": TEAM_ABBREVIATIONS.get(home_team, home_team[:3].upper()),
                                "away_abbr": TEAM_ABBREVIATIONS.get(away_team, away_team[:3].upper())
                            })
                            
                            # Map both teams to this matchup
                            team_to_matchup[home_team] = matchup_key
                            team_to_matchup[away_team] = matchup_key
        
        # Get player-to-team mapping with caching
        try:
            player_team_map = get_player_team_mapping()
            print(f"[INFO] Loaded player-team mapping with {len(player_team_map)} players")
        except Exception as e:
            print(f"[ERROR] Could not load player-team mapping: {e}")
            player_team_map = {}
        
        # Create reverse mapping: team abbreviation -> full team name
        team_abbr_to_full = {}
        for full_name, abbr in TEAM_ABBREVIATIONS.items():
            team_abbr_to_full[abbr] = full_name
        
        # Build matchup team sets for fast lookup
        matchup_teams = {}
        for matchup_info in real_matchups:
            matchup_key = matchup_info['matchup']
            home_team = matchup_info['home_team']  
            away_team = matchup_info['away_team']
            matchup_teams[matchup_key] = {home_team, away_team}
        
        # Group props by STRICT player-team validation
        grouped = {}
        matched_count = 0
        skipped_count = 0
        
        print(f"[DEBUG] Starting strict matchup filtering for {len(props_data)} props")
        print(f"[DEBUG] Available matchups: {list(matchup_teams.keys())}")
        
        for prop in props_data:
            if not isinstance(prop, dict):
                continue
                
            player_name = prop.get('player', '')
            if not player_name:
                continue
            
            # Find player's team using exact or fuzzy matching
            player_team = None
            
            # Exact match first
            if player_name in player_team_map:
                player_team = player_team_map[player_name]
            else:
                # Fuzzy matching for name variations (last name + first initial)
                for mapped_name, team in player_team_map.items():
                    if len(player_name.split()) >= 2 and len(mapped_name.split()) >= 2:
                        prop_last = player_name.split()[-1].lower()
                        prop_first_initial = player_name.split()[0][0].lower()
                        mapped_last = mapped_name.split()[-1].lower()
                        mapped_first_initial = mapped_name.split()[0][0].lower()
                        
                        if (prop_last == mapped_last and 
                            prop_first_initial == mapped_first_initial and 
                            len(prop_last) > 3):
                            player_team = team
                            print(f"[FUZZY] {player_name} -> {mapped_name} ({team})")
                            break
            
            if not player_team:
                skipped_count += 1
                continue
            
            # Find which matchup this player's team belongs to
            matched_matchup = None
            for matchup_key, teams_in_matchup in matchup_teams.items():
                if player_team in teams_in_matchup:
                    matched_matchup = matchup_key
                    break
            
            # Only include prop if player's team is in a real matchup
            if matched_matchup:
                if matched_matchup not in grouped:
                    grouped[matched_matchup] = []
                grouped[matched_matchup].append(prop)
                matched_count += 1
            else:
                skipped_count += 1
        
        # Get game environment classifications with favored team info
        try:
            from odds_api import get_mlb_game_environment_map
            game_environments = get_mlb_game_environment_map()
            print(f"[DEBUG] Loaded {len(game_environments)} game environment classifications")
        except Exception as e:
            print(f"[WARNING] Could not load game environments: {e}")
            game_environments = {}
        
        # Add game environment labels and team status to props
        enhanced_grouped = {}
        for matchup_key, props in grouped.items():
            env_data = game_environments.get(matchup_key, {})
            environment_label = env_data.get('environment', 'Neutral')
            favored_team_abbr = env_data.get('favored_team', '')
            home_team_abbr = env_data.get('home_team', '')
            away_team_abbr = env_data.get('away_team', '')
            
            # Determine underdog team
            underdog_team_abbr = ''
            if favored_team_abbr:
                if favored_team_abbr == home_team_abbr:
                    underdog_team_abbr = away_team_abbr
                elif favored_team_abbr == away_team_abbr:
                    underdog_team_abbr = home_team_abbr
            
            # Create enhanced matchup key with environment label
            if environment_label != 'Neutral':
                enhanced_key = f"{matchup_key} — {environment_label}"
            else:
                enhanced_key = matchup_key
            
            # Enrich each prop with team status information
            enhanced_props = []
            for prop in props:
                # Get player's team from mapping
                player_name = prop.get('player', '')
                player_team_full = player_team_map.get(player_name, '')
                player_team_abbr = TEAM_ABBREVIATIONS.get(player_team_full, player_team_full[:3].upper() if player_team_full else '')
                
                # Determine if player's team is favored
                is_favored = False
                team_status = "unknown"
                
                if favored_team_abbr and player_team_abbr:
                    if player_team_abbr == favored_team_abbr:
                        is_favored = True
                        team_status = "favored"
                    elif player_team_abbr == underdog_team_abbr:
                        is_favored = False
                        team_status = "underdog"
                
                # Enrich prop with team status
                enhanced_prop = prop.copy()
                enhanced_prop.update({
                    "team_abbr": player_team_abbr,
                    "is_favored": is_favored,
                    "team_status": team_status,
                    "favored_team_abbr": favored_team_abbr,
                    "underdog_team_abbr": underdog_team_abbr
                })
                
                # Add true odds calculation using original _attach_fair logic
                try:
                    from probability import fair_probs_from_two_sided, fair_odds_from_prob
                    
                    def set_fair(prop, pA, pB, sideA, sideB):
                        if pA is None: return
                        prop.setdefault("fair", {})
                        prop["fair"]["prob"] = { sideA: round(pA,4), sideB: round(pB,4) }
                        prop["fair"]["american"] = {
                            sideA: fair_odds_from_prob(pA),
                            sideB: fair_odds_from_prob(pB),
                        }

                    # Extract existing odds from current structure and attach fair probabilities
                    shop = enhanced_prop.get("shop") or {}
                    over_am = shop.get("over", {}).get("american")
                    under_am = shop.get("under", {}).get("american")
                    
                    # Totals (Over/Under)
                    if over_am is not None and under_am is not None:
                        p_over, p_under = fair_probs_from_two_sided(float(over_am), float(under_am))
                        set_fair(enhanced_prop, p_over, p_under, "over", "under")
                    
                    # Ensure fair structure exists even if calculation fails
                    if not enhanced_prop.get("fair"):
                        enhanced_prop["fair"] = {
                            "prob": {"over": 0.0, "under": 0.0},
                            "book": ""
                        }
                        
                except Exception as e:
                    print(f"[WARNING] True odds calculation failed for {enhanced_prop.get('player', 'Unknown')}: {e}")
                    enhanced_prop["fair"] = {
                        "prob": {"over": 0.0, "under": 0.0},
                        "book": ""
                    }
                

                
                enhanced_props.append(enhanced_prop)
                
            enhanced_grouped[enhanced_key] = enhanced_props
            print(f"[DEBUG] {enhanced_key}: {len(enhanced_props)} props")
        
        print(f"[DEBUG] Strict filtering results: {matched_count} props matched, {skipped_count} skipped")
        print(f"[DEBUG] Final enhanced matchups: {list(enhanced_grouped.keys())}")
        print(f"[DEBUG] Grouped {len(props_data)} props into {len(enhanced_grouped)} matchups")
        
        return enhanced_grouped
        
    except Exception as e:
        logger.error(f"Error grouping props by matchup: {e}")
        # Fallback: distribute props evenly across common matchups
        try:
            common_matchups = ["BOS @ PHI", "BAL @ CLE", "NYY @ TB", "HOU @ SEA", "LAD @ SF"]
            grouped = {}
            props_per_matchup = max(1, len(props_data) // len(common_matchups))
            
            for i, prop in enumerate(props_data):
                matchup_index = i // props_per_matchup
                if matchup_index >= len(common_matchups):
                    matchup_index = len(common_matchups) - 1
                    
                matchup = common_matchups[matchup_index]
                if matchup not in grouped:
                    grouped[matchup] = []
                grouped[matchup].append(prop)
            
            return grouped
        except:
            return {"All Games": props_data if isinstance(props_data, list) else []}

@app.route("/api/mlb/props")
def get_mlb_props():
    """API endpoint for MLB props with game environment classification"""
    try:
        from enrichment import load_props_from_file
        
        # Load props from file cache
        props_data = load_props_from_file("mlb_props_cache.json")
        
        if not props_data:
            return jsonify({
                "message": "Props are being processed - please check back in a moment",
                "status": "processing", 
                "total_props": 0,
                "matchups": {}
            }), 202
        
        # Group props by matchup with environment labels
        grouped_props = group_props_by_matchup(props_data)
        
        return jsonify({
            "status": "success",
            "total_props": len(props_data),
            "total_matchups": len(grouped_props),
            "matchups": grouped_props
        })
            
    except Exception as e:
        logger.error(f"MLB props API error: {e}")
        return jsonify({
            "message": "Props temporarily unavailable",
            "status": "error",
            "total_props": 0,
            "matchups": {}
        }), 503

@app.route("/player_props")
def get_props():
    """Get enriched props grouped by matchup with optional filtering (Underdog Fantasy style)"""
    try:
        # Check if No-Vig Mode is enabled
        if USE_NOVIG_ONLY:
            league = (request.args.get("league") or "mlb").lower()
            date_iso = request.args.get("date")  # optional "YYYY-MM-DD"
            min_prob = float(request.args.get("min_prob", "0") or 0)
            books_qs = request.args.get("books")
            markets_qs = request.args.get("markets")

            books = [b.strip().lower() for b in books_qs.split(",")] if books_qs else DEFAULT_BOOKS
            markets = [m.strip() for m in markets_qs.split(",")] if markets_qs else None

            raw_offers = fetch_player_prop_offers_flat(league=league, date_iso=date_iso, books=books, markets=markets)
            logger.info(f"[NOVIG] Fetched {len(raw_offers)} raw offers for {league}")
            
            if not raw_offers:
                logger.warning("[NOVIG] No raw offers available, returning empty response")
                return jsonify({}), 200
                
            # Parse new confidence controls
            prioritize_high = (request.args.get("prioritize_high", "true").lower() in ("1","true","yes","on"))
            high_only = (request.args.get("high_only", "0").lower() in ("1","true","yes","on"))
            high_threshold = float(request.args.get("high_threshold", "0.70") or 0.70)
            prefer = (request.args.get("prefer", "over").lower() in ("over","any")) and request.args.get("prefer","over").lower() or "over"
            
            # parse knobs from query
            over_only = (request.args.get("over_only","1").lower() in ("1","true","yes","on"))  # default ON
            
            # Get default overround from environment variable
            default_overround = float(os.getenv("NOVIG_DEFAULT_OVERROUND", "0.04"))
            
            grouped = build_props_novig(
                league, raw_offers,
                prefer_books=books,
                allow_crossbook=True,
                allow_single_side_fallback=True,
                default_overround=default_overround,
                prefer_side=prefer,
                high_threshold=high_threshold
            )
            total_props = sum(len(props) for props in grouped.values())
            logger.info(f"[NOVIG] Built {total_props} props from {len(raw_offers)} offers across {len(grouped)} matchups")

            # optional L10 annotate—doesn't break response if fails
            include_l10 = (request.args.get("include_l10", os.getenv("ENABLE_L10","false")).lower() in ("1","true","yes","on"))
            l10_lookback = int(request.args.get("l10_lookback", os.getenv("L10_LOOKBACK","10")))
            
            if include_l10:
                try:
                    grouped = annotate_props_with_l10(grouped, league=league, lookback=l10_lookback)
                    logger.info(f"[L10] Annotated {total_props} props with L10 trends")
                except Exception as e:
                    logger.warning(f"[L10] annotate failed: {e}")

            # server-side probability filter to keep junk out of UI lists
            if min_prob > 0:
                for mu in list(grouped.keys()):
                    grouped[mu] = [
                        p for p in grouped[mu]
                        if max(p["fair"]["prob"]["over"], p["fair"]["prob"]["under"]) >= min_prob
                    ]
                    if not grouped[mu]:
                        del grouped[mu]

            # High-only filter (optional)
            if high_only:
                tag1 = f"HIGH_OVER_{int(high_threshold*100)}"
                tag2 = f"HIGH_ANY_{int(high_threshold*100)}"
                for mu in list(grouped.keys()):
                    grouped[mu] = [
                        p for p in grouped[mu] 
                        if tag1 in p.get("meta",{}).get("flags",[]) or tag2 in p.get("meta",{}).get("flags",[])
                    ]
                    if not grouped[mu]:
                        del grouped[mu]

            # enforce over_only at the route level too (so it's guaranteed)
            if over_only:
                for mu in list(grouped.keys()):
                    grouped[mu] = [p for p in grouped[mu] if p["fair"]["prob"]["over"] >= float(request.args.get("min_prob","0.0"))]
                    if not grouped[mu]: del grouped[mu]

            # final: they are already sorted by OVER desc inside pairing.py
            return jsonify(grouped), 200
        
        # Standard enrichment flow (existing code)
        from enrichment import load_props_from_file
        
        # Load props from file cache (no Redis dependency)
        props_data = load_props_from_file("mlb_props_cache.json")
        
        if not props_data:
            print("⚠️ No cached props available in file")
            return jsonify({
                "message": "Props are being processed - please check back in a moment",
                "status": "processing", 
                "matchups": {}
            }), 202
        
        # Apply MLB game context enrichment to enhance props with positive environment analysis
        enhanced_context = request.args.get("enhanced_context", "false").lower() == "true"
        if enhanced_context:
            try:
                logger.info("Applying MLB game context enrichment to props")
                props_data = enrich_mlb_props_with_context(props_data)
                logger.info(f"MLB enrichment complete: {len(props_data)} props with positive environment")
            except Exception as e:
                logger.warning(f"MLB enrichment failed, using standard props: {e}")
        
        # Check for matchup filtering
        matchup = request.args.get("matchup")
        if matchup:
            try:
                # Group all props first, then filter by requested matchup
                grouped_props = group_props_by_matchup(props_data)
                
                # Check if the requested matchup exists in our grouped data
                if matchup in grouped_props:
                    matchup_props = grouped_props[matchup]
                    print(f"🎯 Found {len(matchup_props)} props for matchup {matchup}")
                    
                    # Return only the requested matchup
                    filtered_result = {matchup: matchup_props}
                    return jsonify(filtered_result)
                else:
                    # List available matchups for debugging
                    available_matchups = list(grouped_props.keys())
                    print(f"🎯 Matchup '{matchup}' not found. Available: {available_matchups}")
                    return jsonify({"error": f"Matchup '{matchup}' not found. Available matchups: {available_matchups}"}), 404
                
            except Exception as e:
                print(f"🔥 Error filtering props by matchup: {e}")
                return jsonify({"error": "Failed to filter props by matchup"}), 500
        
        # Group props by matchup (no filtering)
        grouped_props = group_props_by_matchup(props_data)
        
        print(f"✅ Serving {len(props_data)} props grouped into {len(grouped_props)} matchups")
        return jsonify(grouped_props)
            
    except Exception as e:
        print(f"🔥 Props endpoint error: {str(e)}")
        return jsonify({
            "message": "Props temporarily unavailable",
            "status": "error",
            "matchups": {}
        }), 503



@app.route("/analytics")
def analytics():
    """Analytics endpoint with hit counting"""
    try:
        hits = cache_incr("hits")
        return jsonify({"hits": hits, "status": "ok"})
    except Exception as e:
        logger.error(f"Error in analytics route: {e}")
        return jsonify({"hits": 0, "status": "error", "error": str(e)})

@app.route("/api/status")
def api_status():
    """API status endpoint - lightweight with minimal operations"""
    try:
        # Check Redis health without blocking
        redis_status = "disconnected"
        if redis_healthy:
            redis_status = "connected"
        elif redis is not None:
            redis_status = "unstable"
        
        # Check initialization status
        initialization_status = "complete" if app_initialized else "in_progress"
        
        return jsonify({
            "message": "Welcome to Mora Bets API!",
            "status": "ok",
            "initialization": initialization_status,
            "redis_connected": redis_healthy,
            "redis_status": redis_status,
            "cache_type": "redis" if redis_healthy else "memory",
            "cache_fallback": "memory" if not redis_healthy else "redis",
            "odds_api_key_set": bool(os.environ.get("ODDS_API_KEY")),
            "custom_analysis_ready": False,  # Placeholder for future custom features
            "system_health": "stable" if redis_healthy and app_initialized else "degraded"
        })
    except Exception as e:
        logger.error(f"Error in status endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/ping")
def ping():
    """Ping endpoint with Redis status for deployment health checks"""
    redis_status = "OK" if redis and redis_healthy else "FAIL"
    return jsonify({"status": "running", "redis": redis_status})

@app.route("/api/odds")
def get_odds():
    """Get cached MLB odds"""
    try:
        cached = cache_get("mlb_odds")
        if cached:
            # Handle bytes, string, or dict data types
            if isinstance(cached, bytes):
                data = json.loads(cached.decode('utf-8'))
            elif isinstance(cached, str):
                data = json.loads(cached)
            else:
                data = cached
            return jsonify(data)
        return jsonify({"error": "Odds not cached yet. Please wait for background job to complete."}), 503
    except Exception as e:
        logger.error(f"Error in odds endpoint: {e}")
        return jsonify({"error": "Failed to retrieve odds"}), 500

@app.route("/api/mlb/environment")
def api_mlb_environment():
    """Get MLB game environment classifications and favored teams"""
    try:
        from odds_api import get_mlb_game_environment_map
        env_map = get_mlb_game_environment_map()
        return jsonify({"environments": env_map})
    except Exception as e:
        logger.error(f"Failed to get MLB environment data: {e}")
        return jsonify({"error": "MLB environment data unavailable"}), 503

@app.route("/api/nfl/environment")
def api_nfl_environment():
    """Get NFL game environment classifications and favored teams"""
    try:
        from nfl_odds_api import get_nfl_game_environment_map
        env_map = get_nfl_game_environment_map()
        return jsonify({"environments": env_map})
    except Exception as e:
        logger.error(f"Failed to get NFL environment data: {e}")
        return jsonify({"error": "NFL environment data unavailable"}), 503

@app.route("/api/mlb/props/enhanced")
def get_enhanced_mlb_props():
    """Get MLB props with deep game context analysis"""
    try:
        from enrichment import load_props_from_file
        
        # Load props from file cache
        props_data = load_props_from_file("mlb_props_cache.json")
        
        if not props_data:
            return jsonify({"error": "No MLB props available"}), 503
        
        # Apply MLB game context enrichment
        enhanced_props = enrich_mlb_props_with_context(props_data)
        
        # Optionally filter to only positive environment props
        filter_positive = request.args.get("positive_only", "true").lower() == "true"
        if filter_positive:
            enhanced_props = filter_positive_environment_props(enhanced_props)
        
        # Group by matchup
        grouped_props = group_props_by_matchup(enhanced_props)
        
        logger.info(f"Enhanced MLB props: {len(enhanced_props)} props with game context")
        return jsonify({
            "total_props": len(enhanced_props),
            "matchups": grouped_props,
            "enrichment_applied": True
        })
        
    except Exception as e:
        logger.error(f"Error in enhanced MLB props endpoint: {e}")
        return jsonify({"error": "Failed to retrieve enhanced MLB props"}), 500

@app.route("/api/nfl/props")
def get_nfl_props():
    """Get NFL player props with graceful off-season handling"""
    try:
        from nfl_odds_api import fetch_nfl_props
        
        # During NFL off-season, handle API errors gracefully
        try:
            raw_props = fetch_nfl_props()
        except RuntimeError as e:
            if "422" in str(e) or "INVALID_MARKET" in str(e):
                logger.info("NFL off-season: No player props available")
                return jsonify([])  # Return empty array instead of error
            raise e
        
        if not raw_props:
            return jsonify([])  # Return empty array for consistency
        
        # Simple transformation for now (can enhance later)
        enhanced_props = []
        for event in raw_props:
            matchup = mk_matchup(event['away_team'], event['home_team'])
            
            for bookmaker in event.get('bookmakers', []):
                for market in bookmaker.get('markets', []):
                    for outcome in market.get('outcomes', []):
                        prop = {
                            'player': outcome.get('description', ''),
                            'stat': market['key'],
                            'line': outcome.get('point', 0),
                            'over_odds': outcome.get('price', 0),
                            'under_odds': 0,  # Would need to find corresponding under
                            'bookmaker': bookmaker['title'],
                            'matchup': matchup,
                            'confidence': 'Medium'  # Default confidence
                        }
                        enhanced_props.append(prop)
        
        return jsonify(enhanced_props)
        
    except Exception as e:
        logger.error(f"Error in NFL props endpoint: {e}")
        return jsonify([])  # Return empty array instead of error for frontend compatibility



@app.route("/api/matchups")
def matchups():
    """Get all matchups with odds - optimized for speed"""
    try:
        data = cache_get("mlb_odds")
        if not data:
            return jsonify({"error": "No cached odds available"}), 503

        # Handle bytes, string, or dict data types
        if isinstance(data, bytes):
            games = json.loads(data.decode('utf-8'))
        elif isinstance(data, str):
            games = json.loads(data)
        else:
            games = data
        
        # Simple matchup format for quick display
        matchups = {}
        
        # Ensure games is a list and contains valid game objects
        if not isinstance(games, list):
            return jsonify({"error": "Invalid game data format"}), 500
            
        for game in games:
            if not isinstance(game, dict):
                continue
                
            home = game.get("home_team")
            away = game.get("away_team")
            if home and away:
                matchup = format_matchup(away, home)
                matchups[matchup] = {
                    "matchup": matchup,
                    "start_time": game.get("commence_time", "Unknown"),
                    "home_team": home,
                    "away_team": away,
                    "home_abbr": get_team_abbreviation(home),
                    "away_abbr": get_team_abbreviation(away)
                }
        
        # Fetch matchup labels (favored team, high-scoring, etc.)
        try:
            from labels import fetch_matchup_labels
            
            league = (request.args.get("league") or "mlb").lower()
            books_qs = request.args.get("books")
            books = [b.strip().lower() for b in books_qs.split(",")] if books_qs else ["draftkings", "fanduel", "betmgm"]

            labels = fetch_matchup_labels(league=league, books=books)
            
            # Attach labels to existing matchups without breaking shape
            for mu, info in labels.items():
                if mu in matchups:
                    # flat keys (safe for existing UI) + namespaced copy
                    matchups[mu]["labels"] = info
                    matchups[mu]["favored_team"]    = info.get("favored_team")
                    matchups[mu]["favored_prob"]    = info.get("favored_prob")
                    matchups[mu]["total_line"]      = info.get("total_line")
                    matchups[mu]["prob_over_total"] = info.get("prob_over_total")
                    matchups[mu]["high_scoring"]    = info.get("high_scoring")
        except Exception as e:
            logger.warning(f"Failed to fetch matchup labels: {e}")
        
        return jsonify(matchups), 200
    except Exception as e:
        logger.error(f"Error in matchups endpoint: {e}")
        return jsonify({"error": "Failed to process matchups"}), 500










@app.route("/debug/cache")
def debug_cache():
    """Debug cache contents"""
    try:
        # Check all cache keys
        cache_keys = []
        if redis_healthy and redis:
            try:
                cache_keys = [k.decode() if isinstance(k, bytes) else k for k in redis.keys("*")]
            except Exception as e:
                logger.error(f"Redis keys error: {e}")
        
        # Count cached props
        cached_props = cache_get("mlb_enriched_props")
        props_count = 0
        if cached_props:
            try:
                props_data = json.loads(cached_props) if isinstance(cached_props, str) else cached_props
                props_count = len(props_data) if isinstance(props_data, list) else 0
            except:
                props_count = 0
        
        return jsonify({
            "cache_keys": cache_keys,
            "memory_cache_keys": list(memory_cache.keys()),
            "redis_healthy": redis_healthy,
            "cached_props_count": props_count,
            "cache_type": "redis" if redis_healthy else "memory"
        })
    except Exception as e:
        logger.error(f"Error in debug cache endpoint: {e}")
        return jsonify({"error": "Failed to debug cache"}), 500

def update_odds():
    """Update MLB odds cache"""
    try:
        logger.info("🔄 Updating MLB odds...")
        games = parse_game_data()
        if games:
            cache_set("mlb_odds", json.dumps(games))
            logger.info(f"Updated MLB odds cache with {len(games)} games")
        else:
            logger.warning("No games data received from odds API")
    except Exception as e:
        logger.error(f"Failed to update odds: {e}")

def update_player_props():
    """Update player props with smart filtering and enrichment"""
    try:
        logger.info("🔄 Starting smart player props update...")
        
        # Step 1: Fetch all available props with proper args
        from datetime import datetime, timezone
        league = "mlb"
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw_props = fetch_player_props(league, date_str)
        logger.info(f"🔍 Total raw props pulled: {len(raw_props)}")
        
        if not raw_props:
            logger.warning("No raw props fetched")
            return []
        
        # Step 2: Smart filtering - only enrich relevant betting props
        logger.info("[DEBUG] Starting smart enrichment for {} props".format(len(raw_props)))
        logger.info("[DEBUG] Filtering {} props for enrichment".format(len(raw_props)))
        
        # Filter for only relevant betting props with smart thresholds
        relevant_props = []
        for prop in raw_props:
            stat_type = prop.get('stat')
            line = float(prop.get('line', 0))
            
            # Smart filtering with appropriate thresholds per stat type (API-verified markets only)
            keep_prop = False
            
            # Batter stats with reasonable thresholds (verified working with Odds API)
            if stat_type == 'batter_hits' and line <= 2.5:
                keep_prop = True
            elif stat_type == 'batter_total_bases' and line <= 1.5:
                keep_prop = True
            elif stat_type == 'batter_home_runs' and line <= 0.5:
                keep_prop = True
            
            # Pitcher stats with reasonable thresholds (verified working with Odds API)
            elif stat_type == 'pitcher_strikeouts' and line <= 7.5:
                keep_prop = True
            elif stat_type == 'pitcher_earned_runs' and line <= 4.5:
                keep_prop = True
            elif stat_type == 'pitcher_hits_allowed' and line <= 8.5:
                keep_prop = True
            elif stat_type == 'pitcher_outs' and line <= 21.5:
                keep_prop = True
            
            if keep_prop:
                relevant_props.append(prop)
        
        logger.info(f"[INFO] Filtered to {len(relevant_props)} relevant betting props (from {len(raw_props)} total)")
        
        # Step 3: Parallel enrichment
        if relevant_props:
            logger.info(f"[INFO] Using ThreadPoolExecutor with 10 workers for {len(relevant_props)} filtered props")
            enriched_props = enrich_player_props(relevant_props)
            
            # Step 4: Cache enriched props to file (Redis-free)
            from enrichment import cache_props_to_file
            cache_props_to_file(enriched_props, "mlb_props_cache.json")
            logger.info(f"✅ Cached {len(enriched_props)} enriched props to file")
            
            return enriched_props
        else:
            logger.warning("No relevant props to enrich")
            return []
            
    except Exception as e:
        logger.error(f"Failed to update player props: {e}")
        logger.error(f"Full traceback: {e}", exc_info=True)
        return []

def redis_health_monitor():
    """Monitor Redis health and attempt reconnection"""
    logger.info("🔄 Attempting scheduled Redis reconnection...")
    check_redis_health()

def system_health_check():
    """Comprehensive system health check"""
    try:
        # Check cache availability
        cache_status = "healthy" if redis_healthy else "degraded"
        
        # Check API key
        api_key_status = "configured" if os.environ.get("ODDS_API_KEY") else "missing"
        
        # Check cached data
        cached_odds = cache_get("mlb_odds")
        cached_props = cache_get("mlb_enriched_props")
        
        odds_count = 0
        props_count = 0
        
        if cached_odds:
            try:
                odds_data = json.loads(cached_odds) if isinstance(cached_odds, str) else cached_odds
                odds_count = len(odds_data) if isinstance(odds_data, list) else 0
            except:
                pass
        
        if cached_props:
            try:
                props_data = json.loads(cached_props) if isinstance(cached_props, str) else cached_props
                props_count = len(props_data) if isinstance(props_data, list) else 0
            except:
                pass
        
        logger.info(f"📊 System Health: Cache={cache_status}, API={api_key_status}, Odds={odds_count}, Props={props_count}")
        
    except Exception as e:
        logger.error(f"System health check failed: {e}")

# Background scheduler setup
scheduler = BackgroundScheduler()

# Schedule jobs
scheduler.add_job(
    func=update_odds,
    trigger="interval",
    hours=2,
    id="update_odds",
    name="Update MLB Odds",
    replace_existing=True
)

# Schedule player props updates 4x daily (DISABLED - using true odds instead of enrichment)
# scheduler.add_job(
#     func=update_player_props,
#     trigger="cron",
#     hour=7,  # 7am PT
#     minute=0,
#     timezone="America/Los_Angeles",
#     id="update_player_props_morning",
#     name="Update Player Props (Morning)",
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_player_props,
#     trigger="cron",
#     hour=12,  # 12pm PT
#     minute=0,
#     timezone="America/Los_Angeles",
#     id="update_player_props_noon",
#     name="Update Player Props (Noon)",
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_player_props,
#     trigger="cron",
#     hour=14,  # 2pm UTC
#     minute=0,
#     timezone="UTC",
#     id="update_player_props_afternoon",
#     name="Update Player Props (Afternoon)",
#     replace_existing=True
# )

# scheduler.add_job(
#     func=update_player_props,
#     trigger="cron",
#     hour=19,  # 7pm UTC
#     minute=0,
#     timezone="UTC",
#     id="update_player_props_evening",
#     name="Update Player Props (Evening)",
#     replace_existing=True
# )

# Health monitoring jobs
scheduler.add_job(
    func=redis_health_monitor,
    trigger="interval",
    seconds=30,
    id="redis_health_monitor",
    name="Redis Health Monitor",
    replace_existing=True
)

scheduler.add_job(
    func=system_health_check,
    trigger="interval",
    minutes=5,
    id="system_health_check",
    name="System Health Check",
    replace_existing=True
)



# Global flag to track initialization
app_initialized = False

def background_initializer():
    """Background initialization of expensive operations"""
    global app_initialized
    import time
    time.sleep(5)  # Wait for server to fully boot
    
    try:
        logger.info("🚀 Starting background initialization...")
        
        # Start scheduler
        if not scheduler.running:
            scheduler.start()
            logger.info("✅ Background scheduler started")
        
        # Initial cache priming (non-blocking)
        logger.info("🔄 Starting cache priming...")
        try:
            update_odds()
            logger.info("✅ Odds cache primed")
        except Exception as e:
            logger.warning(f"Odds cache priming failed: {e}")
        
        try:
            # update_player_props()  # DISABLED - using true odds instead of enrichment
            logger.info("✅ Props cache priming disabled (using true odds)")
        except Exception as e:
            logger.warning(f"Props cache priming failed: {e}")
        
        app_initialized = True
        logger.info("🎉 Background initialization complete")
        
    except Exception as e:
        logger.error(f"Background initialization failed: {e}")
        app_initialized = True  # Mark as complete even if failed



@app.route("/api/nfl/props/debug")
def nfl_props_debug():
    from nfl_odds_api import _detect_nfl_sport_key, fetch_nfl_props
    try:
        sk = _detect_nfl_sport_key()
        data = fetch_nfl_props(hours_ahead=96)
        return jsonify({
            "sport_key": sk,
            "events_with_props": len(data),
            "sample_event": (data[0] if data else None)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -- begin: scheduler wrapper fix --
from datetime import datetime, timezone

def update_player_props_bootstrap():
    try:
        league = "mlb"
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw = fetch_player_props(league, date_str)
        app.logger.info(f"✅ Primed {len(raw or [])} props for {league} {date_str}")
        return len(raw or [])
    except Exception as e:
        app.logger.error(f"Failed to update player props: {e}", exc_info=True)
        return 0
# -- end: scheduler wrapper fix --

# ======== LINE SHOPPING WRAPPER FUNCTIONS ========
def fetch_events_odds(league: str, date_str: str) -> List[Dict[str, Any]]:
    """Wrapper function to fetch events with odds for line shopping"""
    try:
        if league.lower() == "mlb":
            # For MLB, fetch events with odds data
            import requests
            from datetime import datetime, timedelta
            from odds_api import ODDS_API_KEY, BASE_URL, PREFERRED_SPORTSBOOKS
            
            if not ODDS_API_KEY:
                logger.error("ODDS_API_KEY is not set")
                return []
            
            # Parse date and create time window
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")
                start_time = target_date.replace(microsecond=0).isoformat() + "Z"
                end_time = (target_date + timedelta(days=1)).replace(microsecond=0).isoformat() + "Z"
            except ValueError:
                logger.error(f"Invalid date format: {date_str}")
                return []
            
            # Fetch events
            event_resp = requests.get(
                f"{BASE_URL}/sports/baseball_mlb/events",
                params={
                    "apiKey": ODDS_API_KEY,
                    "commenceTimeFrom": start_time,
                    "commenceTimeTo": end_time
                },
                timeout=20
            )
            event_resp.raise_for_status()
            events = event_resp.json()
            
            # For each event, fetch odds data
            events_with_odds = []
            for event in events:
                eid = event.get("id")
                if not eid:
                    continue
                
                try:
                    # Fetch odds for this event
                    odds_resp = requests.get(
                        f"{BASE_URL}/sports/baseball_mlb/events/{eid}/odds",
                        params={
                            "apiKey": ODDS_API_KEY,
                            "regions": "us",
                            "markets": "batter_hits,batter_home_runs,batter_total_bases,pitcher_strikeouts,pitcher_earned_runs,pitcher_outs,pitcher_hits_allowed",
                            "oddsFormat": "american",
                            "bookmakers": ",".join(PREFERRED_SPORTSBOOKS)
                        },
                        timeout=20
                    )
                    odds_resp.raise_for_status()
                    odds_data = odds_resp.json()
                    
                    # Combine event with odds data
                    event_with_odds = {
                        "id": eid,
                        "sport_key": event.get("sport_key"),
                        "sport_title": event.get("sport_title"),
                        "commence_time": event.get("commence_time"),
                        "home_team": event.get("home_team"),
                        "away_team": event.get("away_team"),
                        "bookmakers": odds_data.get("bookmakers", [])
                    }
                    events_with_odds.append(event_with_odds)
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch odds for event {eid}: {e}")
                    continue
            
            return events_with_odds
            
        elif league.lower() == "nfl":
            # For NFL, use the existing NFL odds API
            from nfl_odds_api import fetch_nfl_props
            events = fetch_nfl_props(hours_ahead=96)
            return events
        else:
            return []
    except Exception as e:
        logger.error(f"Error fetching events odds for {league}: {e}")
        return []



def fetch_player_props(league: str, date_str: str) -> List[Dict[str, Any]]:
    """Wrapper function to fetch player props for line shopping"""
    try:
        if league.lower() == "mlb":
            # Load from cache file
            from enrichment import load_props_from_file
            props = load_props_from_file("mlb_props_cache.json")
            
            # Try to match props with actual events for better event_id mapping
            try:
                events = fetch_events_odds(league, date_str)
                if events:
                    # Create a mapping of teams to event IDs
                    team_to_event = {}
                    for event in events:
                        home_team = event.get("home_team", "")
                        away_team = event.get("away_team", "")
                        if home_team and away_team:
                            team_to_event[f"{away_team} @ {home_team}"] = event.get("id")
                    
                    # Try to match props with events based on team names
                    # This is a simplified approach - in a real implementation,
                    # you'd want more sophisticated matching
                    for prop in props:
                        if "event_id" not in prop:
                            # For now, use a placeholder that will be handled by the line shopping logic
                            prop["event_id"] = "mlb_event_placeholder"
            except Exception as e:
                logger.warning(f"Could not match props with events: {e}")
                # Add placeholder event_id if missing
                for prop in props:
                    if "event_id" not in prop:
                        prop["event_id"] = "mlb_event_placeholder"
            
            return props
            
        elif league.lower() == "nfl":
            # For NFL, use the existing NFL props
            from nfl_odds_api import fetch_nfl_props
            events = fetch_nfl_props(hours_ahead=96)
            # Convert events to props format
            props = []
            for event in events:
                for market in event.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        prop = {
                            "player": outcome.get("description", ""),
                            "stat": market.get("key", ""),
                            "line": outcome.get("point"),
                            "odds": outcome.get("price"),
                            "bookmaker": "NFL_Placeholder",
                            "event_id": event.get("id"),
                            "market": market.get("key"),
                            "description": outcome.get("description", "")
                        }
                        props.append(prop)
            return props
        else:
            return []
    except Exception as e:
        logger.error(f"Error fetching player props for {league}: {e}")
        return []

# -- begin: enriched props cache helper --
import os, json
CACHE_DIR = os.getenv("CACHE_DIR", ".")
ENRICHED_FILENAME = os.path.join(CACHE_DIR, "mlb_props_cache.json")

def load_enriched_props(league: str, date_str: str):
    """
    Returns a list of enriched props for the given league/date from on-disk cache.
    We intentionally ignore date boundaries and use the latest snapshot available.
    Each item is expected to include fields like:
      player, team, event_id, market, line, prob_over (or prob_under), shop{over/under{book, american}}
    """
    try:
        with open(ENRICHED_FILENAME, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []
# -- end: enriched props cache helper --

def fetch_line_engine_signals(league: str, date_str: str) -> Dict[str, Any]:
    """
    Engine probabilities for game-level markets.
    Uses market consensus (no-vig) + Poisson Monte Carlo for spread/runline cover.
    """
    try:
        from app import fetch_events_odds  # already exists
    except Exception:
        return {}
    try:
        events = fetch_events_odds(league, date_str) or []
    except Exception:
        events = []
    try:
        from engine_line_signals import build_line_engine_signals
        return build_line_engine_signals(league, date_str, events)
    except Exception:
        return {}

# ======== LINE SHOPPING BLUEPRINT REGISTRATION ========
from routes_line_shopping import line_shop_bp
app.register_blueprint(line_shop_bp)

# ======== EV PLAYS BLUEPRINT REGISTRATION ========
from routes_ev_plays import ev_plays_bp
app.register_blueprint(ev_plays_bp)

# ======== DIAGNOSTICS BLUEPRINT REGISTRATIONS ========
from routes_version import ver_bp
app.register_blueprint(ver_bp)
from routes_introspect import introspect_bp
app.register_blueprint(introspect_bp)
from routes_debug_probe import dbg_bp
app.register_blueprint(dbg_bp)
from routes_ev_debug import evdebug_bp
app.register_blueprint(evdebug_bp)
from routes_ev_simple import evsimple_bp
app.register_blueprint(evsimple_bp)
from routes_ev_diag import evdiag_bp
app.register_blueprint(evdiag_bp)

# ======== L10 TREND BLUEPRINT REGISTRATION ========
from flask import Blueprint
import anyio
from services.sports_l10 import mlb_last10, nfl_last10
from services.l10_summary import summarize_l10

l10_bp = Blueprint("l10", __name__)

@l10_bp.route("/api/l10-trend", methods=["GET"])
def api_l10_trend():
    """
    Query params:
      league = mlb | nfl
      player_id = required for mlb (unless you already support name->id)
      player = optional fallback (if you have resolver)
      market = required (e.g., player_hits)
      line   = required (e.g., 0.5)
    Returns:
      { count, over_rate, avg, series: [{date,opp,value,over}] }
      For NFL (stub) -> count=0, series=[]
    """
    league = (request.args.get("league") or "mlb").lower()
    market = request.args.get("market")
    line   = request.args.get("line")

    if not market or line is None:
        return jsonify({"error": "missing market/line"}), 400

    if league == "mlb":
        player_id = request.args.get("player_id")
        if not player_id:
            return jsonify({"error": "missing player_id for MLB"}), 400
        async def _run():
            games = await mlb_last10(int(player_id))
            return summarize_l10(games, market, line)
        result = anyio.run(_run)
        return jsonify(result)

    if league == "nfl":
        async def _run():
            games = await nfl_last10(request.args.get("player_id") or "")
            return summarize_l10(games, market, line)
        result = anyio.run(_run)
        return jsonify(result)

    return jsonify({"error": "unsupported league"}), 400

app.register_blueprint(l10_bp)

# ======== EVENT CONTEXT BLUEPRINT REGISTRATION ========
from flask import Blueprint, request, jsonify
from services.odds_totals_context import compute_totals_context

ctx_bp = Blueprint("ctx", __name__)

@ctx_bp.route("/api/event-context", methods=["GET"])
def api_event_context():
    """
    params: league=mlb|nfl (default mlb), date=YYYY-MM-DD (optional)
    returns: [{"event_id","start_iso","total_point","true_prob_over","true_prob_under","tier"}, ...]
    """
    league = (request.args.get("league") or "mlb").lower()
    date_str = request.args.get("date")  # optional; if your fetch defaults to today, just pass through
    events_odds = fetch_events_odds(league=league, date_str=date_str) or []  # reuse existing function
    out = []
    for ev in events_odds:
        ctx = compute_totals_context(ev)
        if ctx:
            out.append(ctx)

    # sort by start time if ISO present (string compare works for Zulu ISO)
    out.sort(key=lambda x: x.get("start_iso") or "")
    return jsonify(out)

app.register_blueprint(ctx_bp)



# --- begin: universal canary & diagnostics (idempotent) ---
def __wire_canaries(app):
    # avoid double-wiring
    if getattr(app, "_wired_canaries", False):
        return
    app._wired_canaries = True

    @app.after_request
    def _add_canary_headers(resp):
        resp.headers["X-Wired"] = "true"
        return resp

    @app.get("/__canary")
    def __canary():
        return {"ok": True, "msg": "hello from REAL app"}, 200

    @app.get("/api/_routes")
    def __routes():
        rules=[]
        for r in app.url_map.iter_rules():
            if r.endpoint == "static": continue
            methods=sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")])
            rules.append({"rule": str(r), "endpoint": r.endpoint, "methods": methods})
        rules.sort(key=lambda x: x["rule"])
        return {"count": len(rules), "routes": rules}, 200

    @app.get("/api/_version")
    def __version():
        try:
            import subprocess
            commit = subprocess.check_output(["git","rev-parse","--short","HEAD"]).decode().strip()
            branch = subprocess.check_output(["git","rev-parse","--abbrev-ref","HEAD"]).decode().strip()
        except Exception:
            commit = branch = "unknown"
        return {"branch": branch, "commit": commit}, 200

    @app.get("/ev-debug")
    def __evdebug():
        html = """<!doctype html><meta charset="utf-8"/><title>EV Debug</title>
<pre id="s">Loading…</pre><script>
const d=new Date(),mm=String(d.getMonth()+1).padStart(2,'0'),dd=String(d.getDate()).padStart(2,'0');
const date=`${d.getFullYear()}-${mm}-${dd}`;
(async ()=>{
  const tried=[];
  async function j(u){ const r=await fetch(u); if(!r.ok) throw new Error(u+': '+r.status); return r.json(); }
  let data=null;
  try { tried.push('/api/ev-plays'); data = await j(`/api/ev-plays?league=mlb&date=${date}&novig=1`); }
  catch(e1){
    try { tried.push('/api/ev-plays-simple'); data = await j(`/api/ev-plays-simple?league=mlb&date=${date}`); }
    catch(e2){ document.getElementById('s').textContent='Failed: '+tried.join(' then ')+'\\n'+e2; return; }
  }
  const props=(data.props||[]).length, lines=(data.lines||[]).length;
  document.getElementById('s').textContent = JSON.stringify({date, props, lines, tried}, null, 2);
})();</script>"""
        from flask import Response
        return Response(html, mimetype="text/html")

    # Tolerant EV endpoint (fallback, no blueprints required)
    from flask import request, jsonify
    import datetime, math
    from probability import fair_probs_from_two_sided, fair_odds_from_prob

    def _to_float(x):
        try:
            if isinstance(x, str):
                x = x.strip().replace('%','').replace(',','').replace('+','')
            return float(x)
        except Exception:
            return None

    def _prob(x):
        v = _to_float(x)
        if v is None:
            return None
        if v > 1.0:
            v = v / 100.0
        return v if 0.0 < v < 1.0 else None

    def _american_to_dec(a):
        a = _to_float(a)
        if a is None:
            return None
        a = int(a)
        return 1.0 + (a/100.0 if a > 0 else 100.0/abs(a))

    def _best_price_for_side(item, side):
        """
        Accepts several shapes:
          - item['shop'] = {'over': {'book':'X','american':'+120'}, 'under': {...}}
          - item['odds'] = {'american':'+120','book':'X'}  # assume this is the chosen side
          - item['offers'] = [{'side':'over','book':'X','american':'+120'}, ...]
        Returns dict like {'book':..., 'american': ...} or None.
        """
        side = (side or '').lower()
        shop = item.get('shop') or {}
        if isinstance(shop, dict) and isinstance(shop.get(side), dict):
            it = shop.get(side)
            if it.get('american') is not None:
                return {'book': it.get('book'), 'american': it.get('american')}

        odds = item.get('odds')
        if isinstance(odds, dict) and odds.get('american') is not None:
            # Single odds blob on the record; assume it's for the selected side
            return {'book': odds.get('book') or odds.get('bookmaker'), 'american': odds.get('american')}

        offers = item.get('offers') or item.get('bookmakers')  # tolerate alternative field names
        if isinstance(offers, list):
            # try to pick the first matching side with an american price
            for o in offers:
                s = (o.get('side') or o.get('market') or '').lower()
                if side and side not in s:
                    continue
                am = o.get('american') or (o.get('price') if isinstance(o.get('price'), (int, str, float)) else None)
                if am is not None:
                    return {'book': o.get('book') or o.get('bookmaker'), 'american': am}

        return None

    def _win_probs(item):
        """
        Produce (p_over, p_under) from the most tolerant sources available:
          - item['prob_over'], item['prob_under']
          - item['enriched']['prob_over|prob_under']
          - item['contextual_hit_rate'] (p_over = hit_rate, p_under = 1 - hit_rate)
          - item['fantasy_hit_rate']   (fallback if contextual missing)
        """
        # 1) direct fields
        po = _prob(item.get('prob_over'))
        pu = _prob(item.get('prob_under'))

        # 2) nested enriched
        enr = item.get('enriched') or {}
        if po is None:
            po = _prob(enr.get('prob_over'))
        if pu is None:
            pu = _prob(enr.get('prob_under'))

        # 3) tolerant fallback from hit rates
        if po is None and pu is None:
            hit = _prob(item.get('contextual_hit_rate'))
            if hit is None:
                hit = _prob(item.get('fantasy_hit_rate'))
            if hit is not None:
                po = hit
                pu = 1.0 - hit

        return po, pu

    @app.get("/api/ev-plays-simple")
    def __ev_simple():
        league = (request.args.get("league") or "mlb").lower()
        date_str = request.args.get("date") or datetime.date.today().strftime("%Y-%m-%d")
        min_p = _to_float(request.args.get("min_p") or 0.55) or 0.55
        ev_min = _to_float(request.args.get("ev_min") or 0.01) or 0.01
        debug = request.args.get("debug") == "1"

        # load items from your usual sources
        items = []
        reasons = {"total":0,"no_probs":0,"no_price":0,"below_p":0,"below_ev":0}
        # Prefer enriched loader if you have one; otherwise use your existing fetch.
        try:
            from app import load_enriched_props as _load
            items = _load(league, date_str) or []
        except Exception:
            pass
        if not items:
            try:
                from app import fetch_player_props as _raw
                items = _raw(league, date_str) or []
            except Exception:
                items = []

        out = []
        reasons["total"] = len(items)
        for p in items:
            po, pu = _win_probs(p)
            # decide side by higher probability
            side, winp = None, None
            if po is not None and (pu is None or po >= pu):
                side, winp = "over", po
            elif pu is not None:
                side, winp = "under", pu
            else:
                reasons["no_probs"] += 1
                continue

            if winp < min_p:
                reasons["below_p"] += 1
                continue

            best = _best_price_for_side(p, side)
            if not best or best.get('american') is None:
                reasons["no_price"] += 1
                continue

            dec = _american_to_dec(best.get('american'))
            if dec is None:
                reasons["no_price"] += 1
                continue

            ev = winp * dec - 1.0
            if ev < ev_min:
                reasons["below_ev"] += 1
                continue

            # Add fair probabilities if both over and under odds are available
            fair_data = {}
            shop = p.get("shop") or {}
            over_odds = shop.get("over", {}).get("american")
            under_odds = shop.get("under", {}).get("american")
            
            if over_odds is not None and under_odds is not None:
                try:
                    p_over_fair, p_under_fair = fair_probs_from_two_sided(float(over_odds), float(under_odds))
                    if p_over_fair is not None:
                        fair_data = {
                            "prob": {"over": round(p_over_fair, 4), "under": round(p_under_fair, 4)},
                            "american": {
                                "over": fair_odds_from_prob(p_over_fair),
                                "under": fair_odds_from_prob(p_under_fair),
                            }
                        }
                except Exception as e:
                    # Skip fair calculation if there's an error
                    pass

            out.append({
                "player": p.get("player"),
                "team": p.get("team"),
                "event_id": p.get("event_id") or p.get("game_id"),
                "market": p.get("market") or p.get("stat"),
                "line": p.get("line"),
                "undervalued": {"any": True, "side": side.title()},
                "best": {"side": side.title(), "book": best.get("book"), "american": best.get("american")},
                "metrics": {"p": round(winp,4), "dec": round(dec,4), "ev": round(ev,4)},
                "fair": fair_data
            })

        out.sort(key=lambda r: r["metrics"]["ev"], reverse=True)
        payload = {"date": date_str, "league": league, "props": out, "lines": []}
        if debug:
            payload["debug"] = reasons
        return jsonify(payload), 200
# --- end: universal canary & diagnostics ---

# Wire canaries to the real app
__wire_canaries(app)

# Admin rebuild route
@app.post("/api/admin/rebuild-ev-cache")
def _rebuild_ev_cache():
    try:
        enriched = update_player_props()  # existing function in app
        return jsonify({"enriched_count": len(enriched or [])}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Start background initialization in a separate thread
from threading import Thread
init_thread = Thread(target=background_initializer, daemon=True)
init_thread.start()

# Flask app startup
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)