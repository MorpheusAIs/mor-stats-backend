import requests
import pytest
import time

BASE_URL = "http://127.0.0.1:8000"  # Adjust this if your API is hosted elsewhere

endpoints = [
    "/",
    "/analyze-mor-stakers",
    "/give_mor_reward",
    "/get_stake_info",
    "/total_and_circ_supply",
    "/prices_and_trading_volume",
    "/get_market_cap",
    "/mor_holders_by_range",
    "/locked_and_burnt_mor",
    "/protocol_liquidity",
    "/last_cache_update_time",
    "/capital_metrics",
    "/github_commits",
    "/historical_mor_rewards_locked",
    "/code_metrics",
    "/chain_wise_supplies"
]


@pytest.mark.parametrize("endpoint", endpoints)
def test_endpoint(endpoint):
    url = f"{BASE_URL}{endpoint}"
    start_time = time.time()
    response = requests.get(url)
    end_time = time.time()

    response_time = end_time - start_time

    print(f"\nTesting endpoint: {endpoint}")
    print(f"Status code: {response.status_code}")
    print(f"Response time: {response_time:.2f} seconds")

    assert response.status_code == 200, f"Expected status code 200, but got {response.status_code}"

    # Optional: You can add more specific checks for each endpoint here
    # For example, checking if certain keys exist in the JSON response

    if endpoint == "/total_and_circ_supply":
        assert "data" in response.json(), "Expected 'data' key in response"

    elif endpoint == "/get_market_cap":
        assert "total_supply_market_cap" in response.json(), "Expected 'total_supply_market_cap' key in response"
        assert "circulating_supply_market_cap" in response.json(), ("Expected 'circulating_supply_market_cap' key in "
                                                                    "response")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v"]))
