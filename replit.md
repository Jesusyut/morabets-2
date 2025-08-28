# Mora Bets - Sports Betting Analytics Platform

## Overview
Mora Bets is a sports betting analytics platform focused on MLB and NFL, offering insights through odds analysis, player prop evaluation, and probability calculations. It integrates real-time data from external APIs, processes it with custom algorithms, and delivers analytics via a web interface. The platform aims to provide a competitive edge for serious bettors by offering data-driven insights and advanced betting tools. The platform now focuses exclusively on a single $9.99/month subscription tier with 3-day free trial, removing the previous Mora Assist tier.

### Recent Updates (August 2025)
- **NFL UI Integration**: Added complete MLB/NFL sport switching tabs in dashboard with proper navigation flow
- **MLB Game Context Enrichment**: Implemented advanced game-level context analysis for MLB props, identifying favorable environments for OVER props based on team trends, pitcher matchups, and opponent weaknesses
- **Enhanced Props API**: New `/api/mlb/props/enhanced` endpoint provides deep context analysis with edge calculation
- **NFL Environment Classification**: Fully operational NFL environment API with 272+ games classified as High Scoring (≥50), Low Scoring (≤42), or Neutral
- **Favored Team Highlighting**: Complete integration for both MLB and NFL with bright green (#00FF95) highlighting and sport-specific environment endpoints
- **Production-Ready NFL API**: Updated NFL odds API with proper market normalization, error handling, and The Odds API v4 compliance
- **NFL Off-Season Handling**: Implemented graceful off-season error handling with friendly user messages and proper API response formatting

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture
### Backend
- **Framework**: Flask (Python 3.x)
- **Architecture Pattern**: Monolithic web application with API endpoints.
- **Deployment**: Configured for Replit with Gunicorn WSGI server.
- **Data Processing**: Modules for odds API integration, probability calculations (implied probability, edge, Kelly criterion), contextual player statistics, and fantasy hit rates.
- **Key Features**: Odds analysis, player prop analysis, probability calculations, contextual player performance analysis, and fantasy integration for both MLB and NFL.
- **Multi-Sport Support**: Complete NFL support with production-ready API integration, environment classification, and favored team identification. NFL-specific thresholds and market validation ensure accurate data processing.
- **Caching**: File-based caching (`mlb_props_cache.json`) for persistent data storage, replacing Redis.
- **Background Processing**: APScheduler for automated data refresh (hourly for player props, twice daily for core updates).
- **AI Integration**: OpenAI (GPT-4o-mini) for "Today's Picks" feature, generating parlay recommendations with contextual reasoning.
- **Matchup Validation**: Strict player-team validation ensures accurate prop grouping by MLB matchups.
- **MLB Game Context Engine**: Advanced enrichment layer analyzing team form, pitcher matchups, offensive splits, and bullpen context to identify favorable betting environments with confidence scoring.
- **NFL Environment Classification**: Real-time game environment analysis with NFL-specific scoring thresholds, favored team identification via moneyline analysis, and comprehensive 272+ game coverage.

### Frontend
- **Template Engine**: Jinja2 (Flask's default templating).
- **UI Framework**: Bootstrap 5 with a dark theme.
- **JavaScript**: Vanilla JavaScript for dynamic interactions.
- **Styling**: Custom CSS complements Bootstrap utilities.
- **UI/UX Decisions**: PrizePicks-style interface for player props, professional tabbed navigation (Moneylines, Player Props, Today's Picks, How to Profit), color-coded confidence indicators, mobile responsiveness, user key sign-in functionality, and sport-specific favored team highlighting with bright green (#00FF95) glow effects.
- **Key Features**: Comprehensive search and filtering for player props (player name, stat type, confidence level, sportsbook), educational "How to Profit" section, conversion-optimized landing page with single $9.99 pricing tier, and streamlined user experience focused on the core betting tool.

### System Design
- **Data Flow**: Data ingested from external APIs, processed through analytics modules, cached, and served via Flask endpoints.
- **Robustness**: Implemented robust error handling, graceful degradation (e.g., when AI services are unavailable), and persistent file-based caching for high availability.
- **Security**: Access control via license key verification, integrated with Stripe for subscription management.
- **Scalability**: Optimized API calls through smart filtering, batch processing, and scheduled updates to manage load.
- **SEO**: Comprehensive SEO metadata, Open Graph tags, Twitter Cards, and JSON-LD schema implemented.

## External Dependencies
- **The Odds API**: Primary source for MLB betting odds and lines.
- **MLB Stats API**: Official MLB statistics for player performance data.
- **OpenAI API**: Used for AI-powered "Today's Picks" feature.
- **Stripe**: Payment gateway for subscription management and license key generation.
- **Bootstrap CDN**: Frontend styling and components.
- **Font Awesome**: Icons for the user interface.
- **Meta Pixel**: For analytics and conversion tracking.
- **Python Libraries**: Flask, APScheduler, Requests, Flask-CORS, and other standard libraries for web development and data processing.