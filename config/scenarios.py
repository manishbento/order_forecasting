"""
Scenario Definitions
====================
Define parameter sets for scenario testing and optimization.
Each scenario represents a different forecasting strategy to evaluate.
"""

from itertools import product

# =============================================================================
# DEFAULT SCENARIO (Single Run)
# =============================================================================
DEFAULT_SCENARIO = {
    'scenario_name': 'Default',
    'BASE_COVER': 0.05,
    'BASE_COVER_SOLD_OUT': 0.06,
    'K_FACTOR': 0.25,
    'CASE_SIZE': 6,
    'WEEK_WEIGHTS': (0.6, 0.2, 0.1, 0.1),
    'HIGH_SHRINK_THRESHOLD': 0.15,
    'ROUND_DOWN_SHRINK_THRESHOLD': 0.00
}

# =============================================================================
# SCENARIO TESTING CONFIGURATION
# =============================================================================
# When SCENARIO_TESTING is True, these parameters are used to generate
# all combinations for testing

SCENARIO_TESTING_PARAMS = {
    'base_covers': [0.05],
    'base_cover_shrink': [0.06, 0.07],
    'k_factors': [0.25],
    'case_sizes': [4, 6],
    'week_weights': [
        (0.6, 0.2, 0.1, 0.1),
    ],
    'high_shrink_thresholds': [0.15, 0.20],
    'round_down_shrink_thresholds': [0.00, 0.15]
}


def generate_scenario_sets(params: dict = None) -> list[dict]:
    """
    Generate all combinations of scenario parameters for testing.
    
    Args:
        params: Dictionary of parameter ranges. Uses SCENARIO_TESTING_PARAMS if None.
    
    Returns:
        List of scenario parameter dictionaries
    """
    if params is None:
        params = SCENARIO_TESTING_PARAMS
    
    # Create all combinations
    combinations = product(
        params['base_covers'],
        params['base_cover_shrink'],
        params['k_factors'],
        params['case_sizes'],
        params['week_weights'],
        params['high_shrink_thresholds'],
        params['round_down_shrink_thresholds']
    )
    
    param_sets = []
    for i, (bc, bcs, kf, cs, ww, hst, rdst) in enumerate(combinations, 1):
        param_sets.append({
            'scenario_name': f'Scenario_{i}',
            'BASE_COVER': bc,
            'BASE_COVER_SOLD_OUT': bcs,
            'K_FACTOR': kf,
            'CASE_SIZE': cs,
            'WEEK_WEIGHTS': ww,
            'HIGH_SHRINK_THRESHOLD': hst,
            'ROUND_DOWN_SHRINK_THRESHOLD': rdst
        })
    
    return param_sets


# =============================================================================
# PRE-DEFINED SCENARIOS
# =============================================================================
# Named scenarios for specific business strategies

CONSERVATIVE_SCENARIO = {
    'scenario_name': 'Conservative',
    'BASE_COVER': 0.03,
    'BASE_COVER_SOLD_OUT': 0.04,
    'K_FACTOR': 0.15,
    'CASE_SIZE': 6,
    'WEEK_WEIGHTS': (0.5, 0.3, 0.15, 0.05),
    'HIGH_SHRINK_THRESHOLD': 0.10,
    'ROUND_DOWN_SHRINK_THRESHOLD': 0.05
}

AGGRESSIVE_SCENARIO = {
    'scenario_name': 'Aggressive',
    'BASE_COVER': 0.08,
    'BASE_COVER_SOLD_OUT': 0.10,
    'K_FACTOR': 0.35,
    'CASE_SIZE': 6,
    'WEEK_WEIGHTS': (0.7, 0.15, 0.10, 0.05),
    'HIGH_SHRINK_THRESHOLD': 0.20,
    'ROUND_DOWN_SHRINK_THRESHOLD': 0.00
}

BALANCED_SCENARIO = {
    'scenario_name': 'Balanced',
    'BASE_COVER': 0.05,
    'BASE_COVER_SOLD_OUT': 0.06,
    'K_FACTOR': 0.25,
    'CASE_SIZE': 6,
    'WEEK_WEIGHTS': (0.6, 0.2, 0.1, 0.1),
    'HIGH_SHRINK_THRESHOLD': 0.15,
    'ROUND_DOWN_SHRINK_THRESHOLD': 0.00
}


# List of all pre-defined scenarios
SCENARIOS = [DEFAULT_SCENARIO]


def get_scenarios(scenario_testing: bool = False) -> list[dict]:
    """
    Get the list of scenarios to run.
    
    Args:
        scenario_testing: If True, generates all combinations; if False, returns default only.
    
    Returns:
        List of scenario parameter dictionaries
    """
    if scenario_testing:
        return generate_scenario_sets()
    return [DEFAULT_SCENARIO]
