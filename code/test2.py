from nba_api.stats.endpoints import leaguedashteamstats
import pandas as pd

def fetch_and_merge_team_stats(season):
    base_stats = leaguedashteamstats.LeagueDashTeamStats(season=season, per_mode_detailed='PerGame',
                                                         measure_type_detailed_defense='Base').get_data_frames()[0]
    advanced_stats = leaguedashteamstats.LeagueDashTeamStats(season=season, per_mode_detailed='PerGame',
                                                             measure_type_detailed_defense='Advanced').get_data_frames()[0]

    opp_stats = leaguedashteamstats.LeagueDashTeamStats(season=season, per_mode_detailed='PerGame',
                                                        measure_type_detailed_defense='Opponent').get_data_frames()[0]

    merged_stats = base_stats.merge(advanced_stats, on='TEAM_ID', how='left', suffixes=('', '_advanced'))

    # Merge with opponent stats for Four Factors calculations
    merged_stats = merged_stats.merge(opp_stats[['TEAM_ID', 'OPP_FGA', 'OPP_TOV', 'OPP_OREB', 'OPP_FTA', 'OPP_FGM',
                                                 'OPP_FG3M']], on='TEAM_ID', suffixes=('', '_opp'))

    # Calculate Four Factors
    merged_stats['eFG%'] = (merged_stats['FGM'] + 0.5 * merged_stats['FG3M']) / merged_stats['FGA']
    merged_stats['TOV%'] = merged_stats['TOV'] / (merged_stats['FGA'] + 0.44 * merged_stats['FTA'] + merged_stats['TOV'])
    merged_stats['ORB%'] = merged_stats['OREB'] / (merged_stats['OREB'] + merged_stats['OPP_OREB'])
    merged_stats['FT Rate'] = merged_stats['FTA'] / merged_stats['FGA']
    merged_stats['eFG%_opp'] = (merged_stats['OPP_FGM'] + 0.5 * merged_stats['OPP_FG3M']) / merged_stats['OPP_FGA']
    merged_stats['TOV%_opp'] = merged_stats['OPP_TOV'] / (merged_stats['OPP_FGA'] + 0.44 * merged_stats['OPP_FTA'] + merged_stats['OPP_TOV'])
    merged_stats['ORB%_opp'] = merged_stats['OPP_OREB'] / (merged_stats['OREB'] + merged_stats['OPP_OREB'])
    merged_stats['FT Rate_opp'] = merged_stats['OPP_FTA'] / merged_stats['OPP_FGA']

    merged_stats['Season'] = season

    return merged_stats

def compile_stats_across_seasons(start_year, end_year):
    all_seasons_merged_data = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        season = f"{year}-{str(year+1)[-2:]}"
        print(f"Compiling stats for season: {season}")
        season_merged_stats = fetch_and_merge_team_stats(season)
        all_seasons_merged_data = pd.concat([all_seasons_merged_data, season_merged_stats], ignore_index=True)
    return all_seasons_merged_data

compiled_data = compile_stats_across_seasons(2017, 2023)
csv_file_path = '../data/MSDS_422_FinalProject_Team_Stats_with_Four_Factors.csv'
compiled_data.to_csv(csv_file_path, index=False)
