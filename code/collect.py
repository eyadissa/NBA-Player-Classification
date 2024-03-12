import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats

def fetch_player_stats(season, per_mode='PerGame', measure_type='Base'):
    player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                        season=season,
                        per_mode_detailed=per_mode,
                        measure_type_detailed_defense=measure_type
                    ).get_data_frames()[0]
    

    player_stats['Season'] = season[:4]
    
    player_stats['unique_id'] = player_stats['PLAYER_ID'].astype(str) + "_" + player_stats['Season']
    
    return player_stats

def compile_and_merge_stats_for_season(season):
    per_game_stats = fetch_player_stats(season, 'PerGame', 'Base')
    per_possession_stats = fetch_player_stats(season, 'PerPossession', 'Base')
    totals_stats = fetch_player_stats(season, 'Totals', 'Base')
    advanced_stats = fetch_player_stats(season, 'PerGame', 'Advanced')

    merged_stats = per_game_stats.merge(per_possession_stats.drop(columns=['PLAYER_ID', 'Season']), on='unique_id', how='left', suffixes=('', '_per_possession'))
    merged_stats = merged_stats.merge(totals_stats.drop(columns=['PLAYER_ID', 'Season']), on='unique_id', how='left', suffixes=('', '_totals'))
    merged_stats = merged_stats.merge(advanced_stats.drop(columns=['PLAYER_ID', 'Season']), on='unique_id', how='left', suffixes=('', '_advanced'))
    
    merged_stats.drop(columns=['unique_id'], inplace=True)
    
    return merged_stats

all_seasons_merged_data = pd.DataFrame()

for year in range(2018, 2024):
    season = f"{year}-{str(year+1)[-2:]}" 
    print(f"Compiling stats for season: {season}")
    
    season_merged_stats = compile_and_merge_stats_for_season(season)
    all_seasons_merged_data = pd.concat([all_seasons_merged_data, season_merged_stats], ignore_index=True)

csv_file_path = '../data/nba_players_compiled_stats_2018_2024.csv'
all_seasons_merged_data.to_csv(csv_file_path, index=False)
