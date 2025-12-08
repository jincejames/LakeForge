"""
Simple DSPy Program with Langfuse Visualization

This program demonstrates basic DSPy concepts from the DSPy Playbook:
- Signatures (input/output definitions)
- Modules (reusable components)
- ChainOfThought (step-by-step reasoning)
- Langfuse integration for visualization

=============================================================================
SETUP - Run Langfuse locally with Docker:
=============================================================================

1. Start Langfuse:
   docker run -d --name langfuse \
     -e DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/langfuse \
     -e NEXTAUTH_SECRET=mysecret \
     -e SALT=mysalt \
     -e NEXTAUTH_URL=http://localhost:3000 \
     -p 3000:3000 \
     langfuse/langfuse:latest

   OR use their docker-compose (easier, includes postgres):
   git clone https://github.com/langfuse/langfuse.git
   cd langfuse
   docker compose up -d

2. Open http://localhost:3000, create account, create project, get API keys

3. Set environment variables:
   export LANGFUSE_HOST="http://localhost:3000"
   export LANGFUSE_PUBLIC_KEY="pk-lf-..."
   export LANGFUSE_SECRET_KEY="sk-lf-..."

4. Run: python simple_dspy_program.py

5. View traces at http://localhost:3000
=============================================================================
"""

import logging

import databricks_dspy
import dspy
from openinference.instrumentation.dspy import DSPyInstrumentor

# Configure logging instead of print statements
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# Langfuse Setup - Just enable instrumentation, keys come from env vars
# =============================================================================


def setup_dspy_instrumentation():
    """Enable DSPy tracing - traces are auto-sent to Langfuse via env vars."""
    DSPyInstrumentor().instrument()
    logger.info("DSPy instrumentation enabled - traces will be sent to Langfuse")


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
# Demo execution
# =============================================================================


def run_demos():
    """Run all demo modules - traces will be captured by Langfuse."""

    # Demo 1: Simple QA
    logger.info("=" * 60)
    logger.info("Demo 1: Simple Question Answering")
    logger.info("=" * 60)

    simple_qa = SimpleQA()
    result1 = simple_qa(question="What is the capital of France?")
    logger.info(f"Question: What is the capital of France?")
    logger.info(f"Answer: {result1.answer}")

    # Demo 2: Chain of Thought reasoning (math problem from DSPy Playbook)
    logger.info("=" * 60)
    logger.info("Demo 2: Chain of Thought Reasoning - Math")
    logger.info("=" * 60)

    # Using inline signature for variety (as shown in the playbook)
    math_cot = dspy.ChainOfThought("question -> answer: float")
    result2 = math_cot(question="Two dice are tossed. What is the probability that the sum equals two?")
    logger.info(f"Question: Two dice are tossed. What is the probability that the sum equals two?")
    logger.info(f"Answer: {result2.answer}")

    # Demo 3: Chain of Thought with explicit reasoning
    logger.info("=" * 60)
    logger.info("Demo 3: Chain of Thought with Reasoning Output")
    logger.info("=" * 60)

    reasoning_qa = ReasoningQA()
    result3 = reasoning_qa(question="If a train travels at 60 mph for 2.5 hours, how far does it go?")
    logger.info(f"Question: If a train travels at 60 mph for 2.5 hours, how far does it go?")
    logger.info(f"Reasoning: {result3.reasoning}")
    logger.info(f"Answer: {result3.answer}")

    # Demo 4: Multi-step pipeline
    logger.info("=" * 60)
    logger.info("Demo 4: Multi-Step Pipeline")
    logger.info("=" * 60)

    pipeline = MultiStepPipeline()
    result4 = pipeline(question="Why is the sky blue?")
    logger.info(f"Question: Why is the sky blue?")
    logger.info(f"Full Reasoning: {result4.reasoning}")
    logger.info(f"Answer: {result4.answer}")
    logger.info(f"Reasoning Summary: {result4.reasoning_summary}")

    return result1, result2, result3, result4


def main():
    """Main entry point."""
    # Step 1: Enable DSPy instrumentation (traces auto-sent to Langfuse via env vars)
    setup_dspy_instrumentation()

    # Step 2: Configure DSPy with Databricks LM
    # Uses OAuth U2M by default - run: databricks auth login --host <your-workspace>
    logger.info("Configuring DSPy with Databricks LM...")
    dspy.configure(lm=databricks_dspy.DatabricksLM(model="databricks/databricks-claude-opus-4-5"))

    # Step 3: Run demos - all LLM calls are automatically traced
    logger.info("Starting DSPy demos with Langfuse tracking...")
    results = run_demos()

    logger.info("=" * 60)
    logger.info("All demos completed! Check Langfuse dashboard for visualizations.")
    logger.info("Dashboard: http://localhost:3000 (or your LANGFUSE_HOST)")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    main()

