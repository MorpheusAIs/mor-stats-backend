import requests
from datetime import datetime, timedelta
import json
from app.core.config import GITHUB_API_KEY
from collections import OrderedDict


def fetch_commits(owner, repo, token, since, until):
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    params = {
        "since": since,
        "until": until,
        "per_page": 100
    }

    commit_counts = {}

    try:
        while True:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            commits = response.json()

            if not commits:
                break

            for commit in commits:
                date = datetime.strptime(commit['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ")
                date_str = date.strftime("%d/%m/%Y")
                commit_counts[date_str] = commit_counts.get(date_str, 0) + 1

            if 'Link' in response.headers:
                links = response.headers['Link']
                if 'rel="next"' not in links:
                    break
                params['page'] = params.get('page', 1) + 1
            else:
                break

    except requests.exceptions.RequestException as e:
        print(f"Error fetching commits for {repo}: {str(e)}")
        return None

    return commit_counts


def fetch_all_repos_commits(repos, keys, token, since, until):
    all_commits = {key: {} for key in keys if key != "total"}

    for repo, key in zip(repos, [k for k in keys if k != "total"]):
        commits = fetch_commits("MorpheusAIs", repo.split('/')[-1], token, since, until)
        if commits:
            all_commits[key] = commits
        else:
            print(f"No commits found for {repo}")

    # Calculate total after fetching all commits
    all_commits["total"] = {}
    for repo_data in all_commits.values():
        for date, count in repo_data.items():
            all_commits["total"][date] = all_commits["total"].get(date, 0) + count

    return all_commits


def calculate_cumulative(data):
    cumulative_data = {}
    for key, repo_data in data.items():
        sorted_dates = sorted(repo_data.keys())
        cumulative = 0
        cumulative_data[key] = OrderedDict()
        for date in sorted_dates:
            cumulative += repo_data[date]
            cumulative_data[key][date] = {
                "daily": repo_data[date],
                "cumulative": cumulative
            }
    return cumulative_data


def get_commits_data():
    repos = [
        "MorpheusAIs/Docs",
        "MorpheusAIs/SmartContracts",
        "MorpheusAIs/moragents",
        "MorpheusAIs/MRC",
        "MorpheusAIs/MOR20",
        "MorpheusAIs/Morpheus-Lumerin-Node",
        "MorpheusAIs/DashBoard"
    ]
    keys = [
        "docs",
        "moragents",
        "mrc",
        "mor20",
        "morpheus_lumerin_node",
        "dashboard",
        "total"
    ]
    token = GITHUB_API_KEY
    until_date = datetime.utcnow()
    since_date = until_date - timedelta(days=730)  # Fetching data for the last 2 years

    all_commits_data = fetch_all_repos_commits(repos, keys, token, since_date.isoformat() + 'Z',
                                               until_date.isoformat() + 'Z')

    # Calculate cumulative amounts
    cumulative_data = calculate_cumulative(all_commits_data)

    return cumulative_data


# if __name__ == "__main__":
#     get_commits_data()
