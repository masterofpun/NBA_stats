import requests, requests_cache, json, sqlite3

requests_cache.install_cache('NBA')

req = requests.Session()

DB_FILE = 'data.sqlite'
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

headers = {'User-Agent':'Gathering some stats', 'Accept-Encoding': 'gzip', 'Content-Encoding': 'gzip'}

teamLogUrl = 'http://stats.nba.com/stats/teamgamelog?TeamID={0}&Season={1}-{2}&SeasonType=Regular%20Season'

tY = req.get('http://stats.nba.com/stats/commonTeamYears?LeagueId=00',headers=headers)

gameInfo = 'http://stats.nba.com/stats/boxscoretraditionalv2?GameID={0}&RangeType=0&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0'

teamYears = json.loads(tY.text)['resultSets'][0]['rowSet']

games = {}

c.execute("CREATE TABLE IF NOT EXISTS data (data)")

for team in teamYears:
    
    teamId = team[1]
    minYear = int(team[2])
    maxYear = int(team[3])
    year = minYear
    if teamId == 1610612737:
            continue
        
    while year+1<=maxYear:
        link = teamLogUrl.format(str(teamId),str(year),str(year+1)[2:])
        year = year+1
        
        if teamId == 1610612738 and year<=1949:
            continue
        
        data = req.get(link,headers=headers)
        teamLog = json.loads(data.text)
        season = teamLog['parameters']['Season']
        for row in teamLog['resultSets'][0]['rowSet']:
            site = req.get(gameInfo.format(row[1]),headers=headers)
            data = json.loads(site.text)
            team = data['resultSets'][1]['rowSet']
            try:
                teamData = {
                    'season':season,
                    'game_id':team[0][0],
                    'team_A_team_id':team[0][1],
                    'team_A_team_name':team[0][2],
                    'team_A_team_abbreviation':team[0][3],
                    'team_A_team_city':team[0][4],
                    'team_A_min':team[0][5],
                    'team_A_fgm':team[0][6],
                    'team_A_fga':team[0][7],
                    'team_A_fg_pct':team[0][8],
                    'team_A_fg3m':team[0][9],
                    'team_A_fg3a':team[0][10],
                    'team_A_fg3_pct':team[0][11],
                    'team_A_ftm':team[0][12],
                    'team_A_fta':team[0][13],
                    'team_A_ft_pct':team[0][14],
                    'team_A_oreb':team[0][15],
                    'team_A_dreb':team[0][16],
                    'team_A_reb':team[0][17],
                    'team_A_ast':team[0][18],
                    'team_A_stl':team[0][19],
                    'team_A_blk':team[0][20],
                    'team_A_to':team[0][21],
                    'team_A_pf':team[0][22],
                    'team_A_pts':team[0][23],
                    'team_A_plus_minus':team[0][24],
                    'team_B_team_id':team[1][1],
                    'team_B_team_name':team[1][2],
                    'team_B_team_abbreviation':team[1][3],
                    'team_B_team_city':team[1][4],
                    'team_B_min':team[1][5],
                    'team_B_fgm':team[1][6],
                    'team_B_fga':team[1][7],
                    'team_B_fg_pct':team[1][8],
                    'team_B_fg3m':team[1][9],
                    'team_B_fg3a':team[1][10],
                    'team_B_fg3_pct':team[1][11],
                    'team_B_ftm':team[1][12],
                    'team_B_fta':team[1][13],
                    'team_B_ft_pct':team[1][14],
                    'team_B_oreb':team[1][15],
                    'team_B_dreb':team[1][16],
                    'team_B_reb':team[1][17],
                    'team_B_ast':team[1][18],
                    'team_B_stl':team[1][19],
                    'team_B_blk':team[1][20],
                    'team_B_to':team[1][21],
                    'team_B_pf':team[1][22],
                    'team_B_pts':team[1][23],
                    'team_B_plus_minus':team[1][24]
                    }
            except:
                print(row)
            c.execute('INSERT INTO data VALUES (?)', [json.dumps(teamData)])
        conn.commit()
        print(teamId,season)
c.close()
