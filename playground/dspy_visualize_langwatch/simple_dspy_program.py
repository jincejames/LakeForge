"""
Simple DSPy Program with LangWatch Visualization

This program demonstrates basic DSPy concepts:
- Signatures (input/output definitions)
- Modules (reusable components)
- ChainOfThought (step-by-step reasoning)
- LangWatch integration for visualization

Run: python simple_dspy_program.py
"""

import logging

import databricks_dspy
import dspy
import langwatch

# Configure logging instead of print statements
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# DSPy Signatures - Define input/output schema
# =============================================================================


class QuestionAnswer(dspy.Signature):
    """Answer a question with a concise response."""

    question: str = dspy.InputField(desc="The question to answer")
    answer: str = dspy.OutputField(desc="A concise answer to the question")


class ReasonedAnswer(dspy.Signature):
    """Answer a question with step-by-step reasoning."""

    question: str = dspy.InputField(desc="The question requiring reasoning")
    reasoning: str = dspy.OutputField(desc="Step-by-step reasoning process")
    answer: str = dspy.OutputField(desc="The final answer based on reasoning")


class SummarizeText(dspy.Signature):
    """Summarize a piece of text."""

    text: str = dspy.InputField(desc="The text to summarize")
    summary: str = dspy.OutputField(desc="A concise summary of the text")


# =============================================================================
# DSPy Modules - Reusable components
# =============================================================================


class SimpleQA(dspy.Module):
    """Simple question answering module using basic Predict."""

    def __init__(self):
        super().__init__()
        self.predictor = dspy.Predict(QuestionAnswer)

    def forward(self, question: str) -> dspy.Prediction:
        return self.predictor(question=question)


class ReasoningQA(dspy.Module):
    """Question answering with Chain of Thought reasoning."""

    def __init__(self):
        super().__init__()
        self.chain_of_thought = dspy.ChainOfThought(ReasonedAnswer)

    def forward(self, question: str) -> dspy.Prediction:
        return self.chain_of_thought(question=question)


class MultiStepPipeline(dspy.Module):
    """Multi-step pipeline: Answer -> Summarize the reasoning."""

    def __init__(self):
        super().__init__()
        self.reasoner = dspy.ChainOfThought(ReasonedAnswer)
        self.summarizer = dspy.Predict(SummarizeText)

    def forward(self, question: str) -> dspy.Prediction:
        # Step 1: Get reasoned answer
        result = self.reasoner(question=question)

        # Step 2: Summarize the reasoning
        summary = self.summarizer(text=result.reasoning)

        return dspy.Prediction(
            question=question,
            reasoning=result.reasoning,
            answer=result.answer,
            reasoning_summary=summary.summary,
        )


# =============================================================================
# Main execution with LangWatch tracking
# =============================================================================


@langwatch.trace(name="DSPy Demo Execution")
def run_demos():
    """Run all demo modules with LangWatch tracking."""
    langwatch.get_current_trace().autotrack_dspy()

    # Demo 1: Simple QA
    logger.info("=" * 60)
    logger.info("Demo 1: Simple Question Answering")
    logger.info("=" * 60)

    simple_qa = SimpleQA()
    result1 = simple_qa(question="What is the capital of France?")
    logger.info(f"Question: What is the capital of France?")
    logger.info(f"Answer: {result1.answer}")

    # Demo 2: Chain of Thought reasoning
    logger.info("=" * 60)
    logger.info("Demo 2: Chain of Thought Reasoning")
    logger.info("=" * 60)

    reasoning_qa = ReasoningQA()
    result2 = reasoning_qa(question="If a train travels at 60 mph for 2.5 hours, how far does it go?")
    logger.info(f"Question: If a train travels at 60 mph for 2.5 hours, how far does it go?")
    logger.info(f"Reasoning: {result2.reasoning}")
    logger.info(f"Answer: {result2.answer}")

    # Demo 3: Multi-step pipeline
    logger.info("=" * 60)
    logger.info("Demo 3: Multi-Step Pipeline")
    logger.info("=" * 60)

    pipeline = MultiStepPipeline()
    result3 = pipeline(question="Why is the sky blue?")
    logger.info(f"Question: Why is the sky blue?")
    logger.info(f"Full Reasoning: {result3.reasoning}")
    logger.info(f"Answer: {result3.answer}")
    logger.info(f"Reasoning Summary: {result3.reasoning_summary}")

    return result1, result2, result3


def main():
    """Main entry point."""
    # Configure DSPy with Databricks LM
    # Uses OAuth U2M by default - run: databricks auth login --host <your-workspace>
    logger.info("Configuring DSPy with Databricks LM...")
    dspy.configure(lm=databricks_dspy.DatabricksLM(model="databricks/databricks-claude-opus-4-5"))

    # Initialize LangWatch for visualization
    # First time: will prompt for login and open browser for authentication
    logger.info("Logging into LangWatch (will open browser on first run)...")
    langwatch.login()

    logger.info("Initializing LangWatch DSPy tracking...")
    langwatch.dspy.init(experiment="dspy-visualization-demo", optimizer=None)

    # Run the demos
    logger.info("Starting DSPy demos with LangWatch tracking...")
    results = run_demos()

    logger.info("=" * 60)
    logger.info("All demos completed! Check LangWatch dashboard for visualizations.")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    main()
