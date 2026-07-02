"""
Static model catalog for the Green Cloud Pipeline job generator.

Each entry maps a real model name to:
- job_type_affinity: which job type this model serves
- task: the specific ML.ENERGY task name (used by LLM estimator)
- architecture / params: features for the LLM energy estimator
- gpu_model: simulated hardware — fixed per model, not per job
- output_len_distribution: (mean, std) of output token lengths for sampling
  (empirical values from ML.ENERGY benchmark dataset)
- avg_batch_size: typical concurrent requests for this model/use-case
"""

import random

# ── LLM models ──────────────────────────────────────────────────────────────

LLM_MODELS = [
    {
        "model_name": "meta-llama/Llama-3.1-8B-Instruct",
        "job_type": "interactive_inference",
        "task": "lm-arena-chat",
        "architecture": "Dense Transformer",
        "total_params_billions": 8.0,
        "activated_params_billions": 8.0,
        "gpu_model": "H100",
        "output_len_mean": 512,
        "output_len_std": 180,
        "avg_batch_size": 16,
    },
    {
        "model_name": "meta-llama/Llama-3.1-70B-Instruct",
        "job_type": "interactive_inference",
        "task": "lm-arena-chat",
        "architecture": "Dense Transformer",
        "total_params_billions": 70.0,
        "activated_params_billions": 70.0,
        "gpu_model": "H100",
        "output_len_mean": 600,
        "output_len_std": 200,
        "avg_batch_size": 8,
    },
    {
        "model_name": "mistralai/Mixtral-8x7B-Instruct-v0.1",
        "job_type": "interactive_inference",
        "task": "lm-arena-chat",
        "architecture": "MoE",
        "total_params_billions": 56.0,
        "activated_params_billions": 14.0,
        "gpu_model": "H100",
        "output_len_mean": 480,
        "output_len_std": 160,
        "avg_batch_size": 16,
    },
    {
        "model_name": "meta-llama/Llama-3.1-8B-Instruct",
        "job_type": "interactive_inference",
        "task": "gpqa",
        "architecture": "Dense Transformer",
        "total_params_billions": 8.0,
        "activated_params_billions": 8.0,
        "gpu_model": "H100",
        "output_len_mean": 1024,
        "output_len_std": 300,
        "avg_batch_size": 8,
    },
    {
        "model_name": "meta-llama/Llama-3.1-70B-Instruct",
        "job_type": "interactive_inference",
        "task": "gpqa",
        "architecture": "Dense Transformer",
        "total_params_billions": 70.0,
        "activated_params_billions": 70.0,
        "gpu_model": "B200",
        "output_len_mean": 1100,
        "output_len_std": 320,
        "avg_batch_size": 4,
    },
    {
        "model_name": "meta-llama/Llama-3.1-8B-Instruct",
        "job_type": "interactive_inference",
        "task": "image-chat",
        "architecture": "Dense Transformer",
        "total_params_billions": 8.0,
        "activated_params_billions": 8.0,
        "gpu_model": "H100",
        "output_len_mean": 400,
        "output_len_std": 150,
        "avg_batch_size": 16,
    },
    {
        "model_name": "mistralai/Mixtral-8x7B-Instruct-v0.1",
        "job_type": "interactive_inference",
        "task": "image-chat",
        "architecture": "MoE",
        "total_params_billions": 56.0,
        "activated_params_billions": 14.0,
        "gpu_model": "B200",
        "output_len_mean": 420,
        "output_len_std": 140,
        "avg_batch_size": 16,
    },
]

# ── Diffusion models ─────────────────────────────────────────────────────────

DIFFUSION_MODELS = [
    {
        "model_name": "black-forest-labs/FLUX.1-dev",
        "job_type": "batch_image_gen",
        "task": "text-to-image",
        "model_id": "black-forest-labs/FLUX.1-dev",
        "gpu_model": "H100",
        "typical_batch_size": 4,
    },
    {
        "model_name": "stabilityai/stable-diffusion-3.5-large",
        "job_type": "batch_image_gen",
        "task": "text-to-image",
        "model_id": "stabilityai/stable-diffusion-3.5-large",
        "gpu_model": "B200",
        "typical_batch_size": 8,
    },
    {
        "model_name": "Efficient-Large-Model/SANA1.5_1.6B_1024px_diffusers",
        "job_type": "batch_image_gen",
        "task": "text-to-image",
        "model_id": "Efficient-Large-Model/SANA1.5_1.6B_1024px_diffusers",
        "gpu_model": "H100",
        "typical_batch_size": 4,
    },
]

ALL_MODELS = LLM_MODELS + DIFFUSION_MODELS

# ── Sampling helpers ──────────────────────────────────────────────────────────

def sample_llm_model(task: str = None) -> dict:
    """Sample a random LLM model, optionally filtered by task."""
    pool = [m for m in LLM_MODELS if task is None or m["task"] == task]
    if not pool:
        pool = LLM_MODELS
    return random.choice(pool)


def sample_diffusion_model() -> dict:
    """Sample a random diffusion model."""
    return random.choice(DIFFUSION_MODELS)


def sample_output_token_count(model_entry: dict) -> int:
    """
    Sample expected output token count from model's empirical distribution.
    Uses a normal distribution clipped to a reasonable range.
    This is the one field that's estimated rather than known at arrival —
    equivalent to how real serving systems use historical averages for planning.
    """
    mean = model_entry["output_len_mean"]
    std = model_entry["output_len_std"]
    sampled = int(random.gauss(mean, std))
    return max(64, min(sampled, 4096))  # clip to [64, 4096]


def get_priority_tier(job_type: str) -> str:
    """Derive priority tier from job type. Interactive jobs are always latency-SLA."""
    return "latency_sla" if job_type == "interactive_inference" else "best_effort"
