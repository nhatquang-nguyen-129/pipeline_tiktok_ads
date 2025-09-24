import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.fetch import fetch_campaign_insights

# Fake response giống TikTok API
FAKE_TIKTOK_RESPONSE = {
    "code": 0,
    "message": "OK",
    "data": {
        "list": [
            {
                "metrics": {"clicks": "10", "impressions": "1000", "spend": "5000"},
                "dimensions": {"campaign_id": "123456", "stat_time_day": "2025-09-24 00:00:00"},
            },
            {
                "metrics": {"clicks": "20", "impressions": "2000", "spend": "10000"},
                "dimensions": {"campaign_id": "789012", "stat_time_day": "2025-09-24 00:00:00"},
            },
        ]
    },
}


@patch("src.fetch.secretmanager.SecretManagerServiceClient")
@patch("src.fetch.requests.get")
def test_fetch_campaign_insights(mock_requests_get, mock_secret_client):
    # Mock secret manager
    mock_secret = MagicMock()
    mock_secret.access_secret_version.return_value.payload.data.decode.side_effect = [
        "fake_access_token",  # token
        "fake_advertiser_id",  # advertiser_id
    ]
    mock_secret_client.return_value = mock_secret

    # Mock requests.get trả về fake TikTok API response
    mock_resp = MagicMock()
    mock_resp.json.return_value = FAKE_TIKTOK_RESPONSE
    mock_resp.status_code = 200
    mock_requests_get.return_value = mock_resp

    # Run fetch
    df = fetch_campaign_insights("2025-09-24", "2025-09-24")

    # Kiểm tra DataFrame không rỗng
    assert not df.empty
    assert "campaign_id" in df.columns
    assert "stat_time_day" in df.columns
    assert "impressions" in df.columns
    assert len(df) == 2

    # In thử ra terminal để debug
    print("\n🔍 [TEST DEBUG] DataFrame output:")
    print(df)
