from pydantic import BaseModel, Field
from typing import List
from datetime import datetime


class AgenticAnalysisResult(BaseModel):
    """
    Result of the Layer 2 Agentic Deep Dive Analysis.
    """

    ticker: str
    analysis_date: datetime = Field(default_factory=datetime.utcnow)

    # Scores (0-100)
    accountant_score: float = Field(
        ..., description="Score from The Accountant (Forensic Analysis)"
    )
    strategist_score: float = Field(
        ..., description="Score from The Strategist (Moat & Management)"
    )
    critic_score: float = Field(..., description="Score from The Critic (Review)")
    final_score: float = Field(..., description="Synthesized Hybrid Score")

    # Summaries
    accountant_summary: str = Field(
        ..., description="Key findings from forensic analysis"
    )
    strategist_summary: str = Field(..., description="Key findings from moat analysis")
    critic_critique: str = Field(..., description="Critique of the analysis")
    final_summary: str = Field(
        ..., description="Executive summary of the investment thesis"
    )

    # Flags
    risks_flagged: List[str] = Field(
        default_factory=list, description="List of specific risks identified"
    )
    disqualified: bool = Field(
        default=False,
        description="True if disqualified by any agent (e.g. fraud detected)",
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
