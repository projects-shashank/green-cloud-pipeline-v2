"""
Carbon accounting library.

Units are explicit in every variable name to prevent the
unit-multiplier bug class from v1.

carbon_intensity is in gCO2/kWh (grams of CO2 per kilowatt-hour).
energy is in kWh.
carbon is in grams of CO2.
"""


def compute_carbon_emitted(energy_kwh: float, carbon_intensity_gco2_per_kwh: float) -> float:
    """
    Compute grams of CO2 emitted for a job.

    Args:
        energy_kwh: energy consumed by the job in kilowatt-hours
        carbon_intensity_gco2_per_kwh: grid carbon intensity in gCO2/kWh

    Returns:
        carbon emitted in grams of CO2
    """
    if energy_kwh < 0:
        raise ValueError(f"energy_kwh must be non-negative, got {energy_kwh}")
    if carbon_intensity_gco2_per_kwh < 0:
        raise ValueError(f"carbon_intensity must be non-negative, got {carbon_intensity_gco2_per_kwh}")

    return energy_kwh * carbon_intensity_gco2_per_kwh


def compute_carbon_saved(
    energy_kwh: float,
    origin_carbon_intensity_gco2_per_kwh: float,
    processed_carbon_intensity_gco2_per_kwh: float,
    origin_region: str,
    processed_region: str,
) -> float:
    """
    Compute grams of CO2 saved by redirecting a job to a cleaner region.

    Carbon saved is ONLY credited when origin_region != processed_region.
    A job that started in a clean region and stayed there is normal
    operation -- not a saving. This rule directly prevents the
    double-counting bug from v1.

    Args:
        energy_kwh: energy consumed by the job in kilowatt-hours
        origin_carbon_intensity_gco2_per_kwh: carbon intensity of origin region
        processed_carbon_intensity_gco2_per_kwh: carbon intensity where job ran
        origin_region: region where job arrived
        processed_region: region where job was processed

    Returns:
        carbon saved in grams of CO2 (0.0 if job was not redirected)
    """
    if origin_region == processed_region:
        # Job was not redirected — no saving to credit
        return 0.0

    # Job was redirected — credit the difference
    carbon_at_origin = compute_carbon_emitted(energy_kwh, origin_carbon_intensity_gco2_per_kwh)
    carbon_at_processed = compute_carbon_emitted(energy_kwh, processed_carbon_intensity_gco2_per_kwh)
    saved = carbon_at_origin - carbon_at_processed

    # Sanity check: if we redirected to a dirtier region somehow, saved is negative
    # This should not happen in correct green-mode operation but we log it, not crash
    if saved < 0:
        print(f"WARNING: carbon_saved is negative ({saved:.4f}g). "
              f"Job was redirected from {origin_region} ({origin_carbon_intensity_gco2_per_kwh} gCO2/kWh) "
              f"to {processed_region} ({processed_carbon_intensity_gco2_per_kwh} gCO2/kWh). "
              f"Check routing logic.")
    return saved


def joules_to_kwh(joules: float) -> float:
    """
    Convert joules to kilowatt-hours.
    1 kWh = 3,600,000 joules.
    Isolated as a named function so this conversion
    is never inlined and never gets a wrong multiplier.
    """
    return joules / 3_600_000
