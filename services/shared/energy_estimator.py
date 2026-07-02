import os
import sys
import joblib
import pandas as pd
from pathlib import Path

# Allow running directly or as part of a package
sys.path.insert(0, str(Path(__file__).parent))
from carbon_accounting import joules_to_kwh

_MODELS_DIR = Path(__file__).parent / "models"

def _load_model(filename: str):
    path = _MODELS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Energy model not found at {path}. "
            f"Copy .joblib files to services/shared/models/"
        )
    return joblib.load(path)

_llm_model = None
_diff_model = None

def _get_llm_model():
    global _llm_model
    if _llm_model is None:
        _llm_model = _load_model("llm_energy_model.joblib")
    return _llm_model

def _get_diff_model():
    global _diff_model
    if _diff_model is None:
        _diff_model = _load_model("diff_energy_model.joblib")
    return _diff_model

LLM_FEATURES = [
    "task", "architecture", "total_params_billions",
    "activated_params_billions", "gpu_model", "max_num_seqs", "num_gpus"
]

def _estimate_llm_energy_joules(
    task, architecture, total_params_billions,
    activated_params_billions, gpu_model,
    max_num_seqs, output_token_count, num_gpus=1,
):
    row = pd.DataFrame([{
        "task": task,
        "architecture": architecture,
        "total_params_billions": total_params_billions,
        "activated_params_billions": activated_params_billions,
        "gpu_model": gpu_model,
        "max_num_seqs": max_num_seqs,
        "num_gpus": num_gpus,
    }])
    energy_per_token = float(_get_llm_model().predict(row[LLM_FEATURES])[0])
    return energy_per_token * output_token_count

DIFF_FEATURES = ["model_id", "gpu_model", "batch_size", "num_gpus"]

def _estimate_diffusion_energy_joules(
    model_id, gpu_model, batch_size, num_gpus=1,
):
    row = pd.DataFrame([{
        "model_id": model_id,
        "gpu_model": gpu_model,
        "batch_size": batch_size,
        "num_gpus": num_gpus,
    }])
    energy_per_image = float(_get_diff_model().predict(row[DIFF_FEATURES])[0])
    return energy_per_image * batch_size

def estimate_energy_kwh(job: dict) -> float:
    """
    Estimate energy consumption for a job in kWh.
    Returns kWh. Joule conversion happens here, never inline elsewhere.
    """
    job_type = job["job_type"]

    if job_type == "interactive_inference":
        joules = _estimate_llm_energy_joules(
            task=job["task"],
            architecture=job["architecture"],
            total_params_billions=job["total_params_billions"],
            activated_params_billions=job["activated_params_billions"],
            gpu_model=job["gpu_model"],
            max_num_seqs=job.get("current_concurrency", 16),
            output_token_count=job["expected_output_token_count"],
            num_gpus=1,
        )
    elif job_type == "batch_image_gen":
        joules = _estimate_diffusion_energy_joules(
            model_id=job["model_id"],
            gpu_model=job["gpu_model"],
            batch_size=job["num_images_requested"],
            num_gpus=1,
        )
    else:
        raise ValueError(f"Unknown job_type '{job_type}'.")

    return joules_to_kwh(joules)
