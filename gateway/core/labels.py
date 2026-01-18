"""Label Computation for ML Training.

Compute labels/targets at export time for ML training
as specified in PRD (lines 1648-1703).
"""

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# Label Types
# ─────────────────────────────────────────────────────────────────────────────


class LabelType(str, Enum):
    """Types of labels that can be computed."""

    RETURN = "return"  # (close[t+n] - close[t]) / close[t]
    LOG_RETURN = "log_return"  # log(close[t+n] / close[t])
    DIRECTION = "direction"  # sign(return)
    VOLATILITY = "volatility"  # std(returns) over window
    MAX_RETURN = "max_return"  # max(returns) over window
    MIN_RETURN = "min_return"  # min(returns) over window
    HIT_TARGET = "hit_target"  # 1 if return > threshold else 0


# ─────────────────────────────────────────────────────────────────────────────
# Label Configuration
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class LabelConfig:
    """Configuration for a single label."""

    name: str  # Column name for the label
    type: LabelType
    lookahead: str | None = None  # "5m", "1h", "1d"
    window: str | None = None  # For volatility
    threshold: float | None = None  # For hit_target

    def to_dict(self) -> dict[str, Any]:
        result = {
            "name": self.name,
            "type": self.type.value,
        }
        if self.lookahead:
            result["lookahead"] = self.lookahead
        if self.window:
            result["window"] = self.window
        if self.threshold is not None:
            result["threshold"] = self.threshold
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "LabelConfig":
        return cls(
            name=data["name"],
            type=LabelType(data["type"]),
            lookahead=data.get("lookahead"),
            window=data.get("window"),
            threshold=data.get("threshold"),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Label Computer
# ─────────────────────────────────────────────────────────────────────────────


class LabelComputer:
    """Computes ML labels from price data.

    Supports various label types for supervised learning:
    - Forward returns (simple and log)
    - Direction (up/down/flat)
    - Volatility (rolling standard deviation)
    - High/low watermarks
    - Target hit detection

    Example:
        computer = LabelComputer()

        labels = [
            LabelConfig("return_5m", LabelType.RETURN, lookahead="5m"),
            LabelConfig("direction_1h", LabelType.DIRECTION, lookahead="1h"),
        ]

        labeled_data = computer.apply_labels(bars_df, labels)

    CAUTION: Labels use future data. The last `lookahead` rows will have null labels.
    Only use labels for training, never inference.
    """

    # Duration string to bar count (assuming 1-minute bars)
    DURATION_MAP = {
        "1m": 1,
        "5m": 5,
        "10m": 10,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "1d": 390,  # Regular session bars
        "1w": 1950,  # 5 trading days
    }

    def compute_return(
        self,
        closes: list[float],
        lookahead: int,
    ) -> list[float | None]:
        """Compute simple forward return.

        Formula: (close[t+n] - close[t]) / close[t]

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead

        Returns:
            List of returns (None for last `lookahead` entries)
        """
        n = len(closes)
        result: list[float | None] = []

        for i in range(n):
            if i + lookahead >= n:
                result.append(None)
            else:
                current = closes[i]
                future = closes[i + lookahead]
                # Avoid division by zero and invalid -100% returns
                if current != 0 and future != 0:
                    result.append((future - current) / current)
                else:
                    result.append(None)

        return result

    def compute_log_return(
        self,
        closes: list[float],
        lookahead: int,
    ) -> list[float | None]:
        """Compute log forward return.

        Formula: log(close[t+n] / close[t])

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead

        Returns:
            List of log returns (None for last `lookahead` entries)
        """
        n = len(closes)
        result: list[float | None] = []

        for i in range(n):
            if i + lookahead >= n:
                result.append(None)
            else:
                current = closes[i]
                future = closes[i + lookahead]
                if current > 0 and future > 0:
                    result.append(math.log(future / current))
                else:
                    result.append(None)

        return result

    def compute_direction(
        self,
        closes: list[float],
        lookahead: int,
    ) -> list[int | None]:
        """Compute direction of return.

        Returns: -1 (down), 0 (flat), 1 (up)

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead

        Returns:
            List of directions (None for last `lookahead` entries)
        """
        returns = self.compute_return(closes, lookahead)
        result: list[int | None] = []

        for r in returns:
            if r is None:
                result.append(None)
            elif r > 0.0001:  # Small threshold to avoid noise
                result.append(1)
            elif r < -0.0001:
                result.append(-1)
            else:
                result.append(0)

        return result

    def compute_volatility(
        self,
        closes: list[float],
        window: int,
    ) -> list[float | None]:
        """Compute rolling volatility (standard deviation of returns).

        Args:
            closes: List of close prices
            window: Window size for rolling calculation

        Returns:
            List of volatility values (None for first `window-1` entries)
        """
        n = len(closes)

        # First compute returns
        returns: list[float | None] = []
        for i in range(n):
            if i == 0:
                returns.append(None)
            else:
                prev = closes[i - 1]
                curr = closes[i]
                if prev > 0:
                    returns.append((curr - prev) / prev)
                else:
                    returns.append(None)

        # Then compute rolling std
        result: list[float | None] = []
        for i in range(n):
            if i < window:
                result.append(None)
            else:
                window_returns = [r for r in returns[i - window + 1 : i + 1] if r is not None]
                if len(window_returns) >= 2:
                    mean = sum(window_returns) / len(window_returns)
                    variance = sum((r - mean) ** 2 for r in window_returns) / (
                        len(window_returns) - 1
                    )
                    result.append(math.sqrt(variance))
                else:
                    result.append(None)

        return result

    def compute_max_return(
        self,
        closes: list[float],
        lookahead: int,
    ) -> list[float | None]:
        """Compute maximum return over lookahead window.

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead

        Returns:
            List of max returns (None for last `lookahead` entries)
        """
        n = len(closes)
        result: list[float | None] = []

        for i in range(n):
            if i + lookahead >= n:
                result.append(None)
            else:
                current = closes[i]
                if current == 0:
                    result.append(None)
                    continue

                max_price = max(closes[i + 1 : i + lookahead + 1])
                result.append((max_price - current) / current)

        return result

    def compute_min_return(
        self,
        closes: list[float],
        lookahead: int,
    ) -> list[float | None]:
        """Compute minimum return over lookahead window.

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead

        Returns:
            List of min returns (None for last `lookahead` entries)
        """
        n = len(closes)
        result: list[float | None] = []

        for i in range(n):
            if i + lookahead >= n:
                result.append(None)
            else:
                current = closes[i]
                if current == 0:
                    result.append(None)
                    continue

                min_price = min(closes[i + 1 : i + lookahead + 1])
                result.append((min_price - current) / current)

        return result

    def compute_hit_target(
        self,
        closes: list[float],
        lookahead: int,
        threshold: float,
    ) -> list[int | None]:
        """Compute whether return hits target threshold.

        Returns: 1 if return > threshold, else 0

        Args:
            closes: List of close prices
            lookahead: Number of bars to look ahead
            threshold: Return threshold (e.g., 0.01 for 1%)

        Returns:
            List of 0/1 values (None for last `lookahead` entries)
        """
        returns = self.compute_return(closes, lookahead)
        result: list[int | None] = []

        for r in returns:
            if r is None:
                result.append(None)
            elif r >= threshold:
                result.append(1)
            else:
                result.append(0)

        return result

    def apply_labels(
        self,
        data: list[dict],
        labels: list[LabelConfig],
        close_field: str = "close",
    ) -> list[dict]:
        """Apply multiple labels to data.

        Args:
            data: List of bar dictionaries
            labels: List of label configurations
            close_field: Name of close price field

        Returns:
            Data with label columns added
        """
        if not data:
            return data

        # Extract closes
        closes = [float(bar.get(close_field, bar.get("c", 0))) for bar in data]

        # Compute each label
        label_values: dict[str, list] = {}

        for label in labels:
            lookahead = self._parse_duration(label.lookahead) if label.lookahead else 1
            window = self._parse_duration(label.window) if label.window else 20

            if label.type == LabelType.RETURN:
                label_values[label.name] = self.compute_return(closes, lookahead)
            elif label.type == LabelType.LOG_RETURN:
                label_values[label.name] = self.compute_log_return(closes, lookahead)
            elif label.type == LabelType.DIRECTION:
                label_values[label.name] = self.compute_direction(closes, lookahead)
            elif label.type == LabelType.VOLATILITY:
                label_values[label.name] = self.compute_volatility(closes, window)
            elif label.type == LabelType.MAX_RETURN:
                label_values[label.name] = self.compute_max_return(closes, lookahead)
            elif label.type == LabelType.MIN_RETURN:
                label_values[label.name] = self.compute_min_return(closes, lookahead)
            elif label.type == LabelType.HIT_TARGET:
                threshold = label.threshold or 0.01
                label_values[label.name] = self.compute_hit_target(closes, lookahead, threshold)

        # Add labels to data
        result = []
        for i, bar in enumerate(data):
            new_bar = dict(bar)
            for label_name, values in label_values.items():
                new_bar[label_name] = values[i]
            result.append(new_bar)

        return result

    def _parse_duration(self, duration: str | None) -> int:
        """Parse duration string to bar count.

        Args:
            duration: Duration string (e.g., "5m", "1h", "1d")

        Returns:
            Number of 1-minute bars
        """
        if not duration:
            return 1

        duration = duration.lower().strip()

        # Check predefined map
        if duration in self.DURATION_MAP:
            return self.DURATION_MAP[duration]

        # Try parsing custom format
        import re

        match = re.match(r"(\d+)([mhd])", duration)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            if unit == "m":
                return value
            elif unit == "h":
                return value * 60
            elif unit == "d":
                return value * 390

        return 1


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_computer: LabelComputer | None = None


def get_label_computer() -> LabelComputer:
    """Get singleton LabelComputer instance."""
    global _computer
    if _computer is None:
        _computer = LabelComputer()
    return _computer
