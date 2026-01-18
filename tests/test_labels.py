"""Unit tests for Label Computation."""

import pytest

from gateway.core.labels import (
    LabelComputer,
    LabelConfig,
    LabelType,
    get_label_computer,
)


class TestLabelType:
    """Test LabelType enum."""

    def test_all_types_exist(self):
        assert LabelType.RETURN.value == "return"
        assert LabelType.LOG_RETURN.value == "log_return"
        assert LabelType.DIRECTION.value == "direction"
        assert LabelType.VOLATILITY.value == "volatility"
        assert LabelType.MAX_RETURN.value == "max_return"
        assert LabelType.MIN_RETURN.value == "min_return"
        assert LabelType.HIT_TARGET.value == "hit_target"


class TestLabelConfig:
    """Test LabelConfig dataclass."""

    def test_to_dict(self):
        config = LabelConfig(
            name="return_5m",
            type=LabelType.RETURN,
            lookahead="5m",
        )
        d = config.to_dict()
        assert d["name"] == "return_5m"
        assert d["type"] == "return"
        assert d["lookahead"] == "5m"

    def test_from_dict(self):
        data = {
            "name": "direction_1h",
            "type": "direction",
            "lookahead": "1h",
        }
        config = LabelConfig.from_dict(data)
        assert config.name == "direction_1h"
        assert config.type == LabelType.DIRECTION


class TestLabelComputer:
    """Test LabelComputer."""

    def setup_method(self):
        self.computer = LabelComputer()
        # Sample price series: 100 -> 102 -> 104 -> 103 -> 105
        self.closes = [100.0, 102.0, 104.0, 103.0, 105.0]

    # Return
    def test_compute_return(self):
        returns = self.computer.compute_return(self.closes, lookahead=1)
        assert len(returns) == 5
        assert returns[0] == pytest.approx(0.02, rel=0.01)  # (102-100)/100
        assert returns[1] == pytest.approx(0.0196, rel=0.01)  # (104-102)/102
        assert returns[-1] is None  # Last element has no lookahead

    def test_compute_return_lookahead_2(self):
        returns = self.computer.compute_return(self.closes, lookahead=2)
        assert returns[0] == pytest.approx(0.04, rel=0.01)  # (104-100)/100
        assert returns[-1] is None
        assert returns[-2] is None

    # Log return
    def test_compute_log_return(self):
        returns = self.computer.compute_log_return(self.closes, lookahead=1)
        assert len(returns) == 5
        assert returns[0] is not None
        assert returns[-1] is None

    # Direction
    def test_compute_direction(self):
        directions = self.computer.compute_direction(self.closes, lookahead=1)
        assert directions[0] == 1  # 100 -> 102, up
        assert directions[1] == 1  # 102 -> 104, up
        assert directions[2] == -1  # 104 -> 103, down
        assert directions[-1] is None

    # Volatility
    def test_compute_volatility(self):
        vol = self.computer.compute_volatility(self.closes, window=3)
        assert vol[0] is None  # Not enough data
        assert vol[1] is None
        assert vol[2] is None
        assert vol[3] is not None  # Now have 3 returns

    # Max return
    def test_compute_max_return(self):
        max_returns = self.computer.compute_max_return(self.closes, lookahead=2)
        # From 100, max over next 2 bars is 104, return = 4%
        assert max_returns[0] == pytest.approx(0.04, rel=0.01)
        assert max_returns[-1] is None
        assert max_returns[-2] is None

    # Min return
    def test_compute_min_return(self):
        min_returns = self.computer.compute_min_return(self.closes, lookahead=2)
        # From 100, min over next 2 bars is 102, return = 2%
        assert min_returns[0] == pytest.approx(0.02, rel=0.01)

    # Hit target
    def test_compute_hit_target(self):
        hits = self.computer.compute_hit_target(self.closes, lookahead=1, threshold=0.015)
        assert hits[0] == 1  # 2% > 1.5%
        assert hits[1] == 1  # ~1.96% > 1.5%
        assert hits[2] == 0  # -0.96% < 1.5%

    # Apply labels
    def test_apply_labels(self):
        data = [
            {"close": 100.0, "timestamp": "2024-01-15T10:00:00Z"},
            {"close": 102.0, "timestamp": "2024-01-15T10:01:00Z"},
            {"close": 104.0, "timestamp": "2024-01-15T10:02:00Z"},
            {"close": 103.0, "timestamp": "2024-01-15T10:03:00Z"},
            {"close": 105.0, "timestamp": "2024-01-15T10:04:00Z"},
        ]

        labels = [
            LabelConfig("ret_1m", LabelType.RETURN, lookahead="1m"),
            LabelConfig("dir_1m", LabelType.DIRECTION, lookahead="1m"),
        ]

        result = self.computer.apply_labels(data, labels)

        assert len(result) == 5
        assert "ret_1m" in result[0]
        assert "dir_1m" in result[0]
        assert result[0]["ret_1m"] is not None
        assert result[-1]["ret_1m"] is None  # No lookahead

    def test_apply_labels_with_hit_target(self):
        data = [
            {"close": 100.0},
            {"close": 102.0},
            {"close": 99.0},
        ]

        labels = [
            LabelConfig("hit_2pct", LabelType.HIT_TARGET, lookahead="1m", threshold=0.02),
        ]

        result = self.computer.apply_labels(data, labels)
        assert result[0]["hit_2pct"] == 1  # 2% exactly hits
        assert result[1]["hit_2pct"] == 0  # Goes down

    # Duration parsing
    def test_parse_duration(self):
        assert self.computer._parse_duration("5m") == 5
        assert self.computer._parse_duration("1h") == 60
        assert self.computer._parse_duration("1d") == 390

    # Edge cases
    def test_empty_data(self):
        result = self.computer.apply_labels([], [])
        assert result == []

    def test_zero_prices(self):
        closes = [100.0, 0.0, 100.0]
        returns = self.computer.compute_return(closes, lookahead=1)
        assert returns[0] is None  # Division by zero avoided

    # Singleton
    def test_singleton(self):
        c1 = get_label_computer()
        c2 = get_label_computer()
        assert c1 is c2
