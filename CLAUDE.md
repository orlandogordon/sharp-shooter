# NFL Betting Data Workflow Requirements

## Architecture Decision
- **Weekly Google Sheets files** for immediate tracking
- **Migrate to database** by midseason for comprehensive analysis
- **Export weekly data** to prepare for database migration

## File Structure
```
Week_1_NFL_Betting_2024.xlsx
├── Overview (Dashboard)
├── Game_1_Lines_Props
├── Game_2_Lines_Props
├── ...
├── My_Picks
└── Results
```

## Data Schema Requirements

### Line Tracking with Snapshots
- Track **3-4 snapshots** of each line throughout the week
- Columns: `Line_Snapshot_1`, `Line_Snapshot_2`, `Line_Snapshot_3`, `Line_Snapshot_4`
- Timestamp columns: `Collected_Date_1`, `Collected_Date_2`, etc.

### Enhanced Pick Tracking
- **Comments**: Free-text commentary field
- **Pick_Type**: `"manual"` | `"model"` | `"system"`
- **Tags**: Comma-separated tags (e.g., `"weather_play,home_dog,revenge_game"`)
- **Confidence**: 1-10 scale
- **Reasoning**: Text explanation

### Core Data Tables

#### Game Lines (per game tab)
```
Game_ID, Date, Home_Team, Away_Team, Bookmaker,
Spread_Line_1, Spread_Odds_1, Collected_Date_1,
Spread_Line_2, Spread_Odds_2, Collected_Date_2,
[...repeat for 4 snapshots...],
Total_Line_1, Total_Odds_1, [timestamps],
ML_Home_1, ML_Away_1, [timestamps]
```

#### Player Props (per game tab)  
```
Game_ID, Player_Name, Market_Type, Bookmaker,
Over_Line_1, Over_Odds_1, Under_Line_1, Under_Odds_1, Collected_Date_1,
[...repeat for 4 snapshots...]
```

#### My Picks (weekly summary tab)
```
Pick_ID, Game_ID, Bet_Type, Selection, Line_Value, Odds,
Stake, Confidence, Pick_Type, Tags, Comments, Reasoning,
Result, Profit_Loss, Date_Placed, Date_Resolved
```

## Data Collection Schedule
- **Tuesday**: Opening lines (Snapshot 1)
- **Thursday**: Mid-week update (Snapshot 2)  
- **Saturday**: Pre-game lines (Snapshot 3)
- **Sunday AM**: Final lines (Snapshot 4)

## Database Migration Prep
- **Tag normalization**: Parse comma-separated tags for relational structure
- **Pick type categorization**: Ensure consistent values
- **Line movement analysis**: Calculate movement between snapshots
- **Export format**: CSV with consistent column naming

## Next Steps

### Phase 1: Setup (Week 1-2)
1. Create Google Sheets template with tab structure
2. Build API collection script for 4 daily snapshots
3. Set up automated weekly file creation
4. Design pick entry workflow

### Phase 2: Operations (Week 3-8)  
1. Weekly data collection and pick tracking
2. Refine tag taxonomy based on actual usage
3. Monitor file performance and data quality
4. Build export/analysis scripts

### Phase 3: Database Migration (Week 8-10)
1. Design normalized database schema
2. Build ETL pipeline from weekly sheets
3. Create analysis and reporting tools
4. Migrate to production database workflow

## Technical Requirements
- **Google Sheets API** for automated file creation
- **The Odds API** for line collection (budget: ~200 calls/week)
- **Python scripts** for data processing and export
- **PostgreSQL** for eventual database migration
- **Data validation** to ensure snapshot consistency

## Success Metrics
- Complete line movement history (4 snapshots per line)
- 100% pick tracking with enhanced metadata
- Clean data export ready for database migration
- Scalable workflow for multiple sports/seasons

## the-odds-api documentation
- Code Samples: https://the-odds-api.com/liveapi/guides/v4/samples.html
- Main page of documentation: https://the-odds-api.com/liveapi/guides/v4/#overview
- Market parameter options: https://the-odds-api.com/sports-odds-data/betting-markets.html