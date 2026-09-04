# Rugby DB Analysis - Project Context

## Overview
Rugby statistics scraping and analysis project. Scrapes match, player, and team data from ESPN across multiple professional rugby leagues. Data is stored as CSVs and analyzed via Jupyter notebooks.

## Leagues
| Key | Competition |
|-----|------------|
| URC | United Rugby Championship |
| Prem | English Premiership |
| T14 | Top 14 (French league) |
| SR | Super Rugby |
| SixNat | Six Nations |
| RChamp | Rugby Championship |
| ChampCup | Champions Cup |
| RWC | Rugby World Cup |
| PNCup | Pacific Nations Cup |

## Repository Structure
```
Rugby_DB_Analysis/
├── scrape_code/
│   ├── scrapers/               # Core scraping Python files
│   │   ├── scraper.py          # Main scrape_model class (Selenium + BeautifulSoup)
│   │   ├── scrape_test.py      # Standalone scrape testing script
│   │   ├── tester_script_funcs.py
│   │   └── prod_scrape.py      # Production scrape runner (stub)
│   ├── utils/                  # Utility modules
│   │   ├── util_funcs.py       # Low-level parsing utilities (proportion values, etc.)
│   │   └── util_data_funcs.py  # Data aggregation: gather_dataframes() per league
│   ├── notebooks/
│   │   ├── testing/            # Notebooks for testing scraper code
│   │   │   ├── scraper_test_v2.ipynb
│   │   │   ├── scrape_unittest.ipynb
│   │   │   ├── odds_scrape_test.ipynb
│   │   │   └── playwright_testing.ipynb
│   │   ├── data_ops/           # Notebooks for updating and debugging data
│   │   │   ├── data_upkeep.ipynb
│   │   │   └── rugby_scrape_nb.ipynb
│   │   └── analysis/           # Analysis and modelling notebooks
│   │       ├── win_prob_model.ipynb
│   │       ├── stat_test.ipynb
│   │       └── lr_eda.ipynb
│   ├── archive/                # Stray/experimental files (index.html, etc.)
│   ├── formed_data/
│   │   ├── game_data_v2/       # Per-game stats CSVs (current)
│   │   ├── player_data_mk2/    # Player stats (current)
│   │   ├── team_data/          # Team-level stats CSVs
│   │   ├── game_schedule_data/ # Schedule/fixture data
│   │   ├── match_comm_data/    # Match commentary data (exploratory)
│   │   └── archive/            # Legacy data (game_data/, player_data/, team_data_v2/)
│   └── test_data/              # Raw test/intermediate CSV data
├── outputs/                    # Generated figures, images, and output files
└── blog_post_drafts/           # Draft blog posts (markdown)
```

## Key Code Patterns

### scrape_model class (`scraper.py`)
- Initialized with `league_set`, `season_set`, `date_set`, `update_type`, `driver_type`
- `driver_type='backend'` runs headless Chrome; `driver_type='testing'` opens visible browser
- Uses Selenium ChromeDriver + BeautifulSoup for ESPN scraping

### Data Loading (`util_data_funcs.py`)
- `gather_dataframes(league)` loads all CSVs for a league and returns `(game_df, player_df, team_df)`
- Reads from `formed_data/game_data_v2/`, `formed_data/player_data_mk2/`, `formed_data/team_data/`

### CSV Naming Convention
`{LeagueKey}_{data_type}_{year}.csv`
Examples: `URC_game_stats_2024.csv`, `T14_player_stats_2023.csv`, `SR_team_data_2024.csv`

## Dependencies
- `pandas`, `numpy`, `requests`, `beautifulsoup4`
- `selenium` (ChromeDriver, headless Chrome)
- `matplotlib`
- `tqdm`

## Git
- Active branch: `v2_branch`
- Main branch: `main`
