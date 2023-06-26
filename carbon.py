#Input needs to be in json 
#Output Overall score is a % number
#inputs are fuel_stacking, primary_fuel, improved_cookstove, burn_cookstove, household_size, meals_per_day 
def calculate_overall_score(answers): 
    fuel_stacking = answers.get('fuel_stacking', [])
    fuel_stacking_score = 0.2 if 'charcoal' in fuel_stacking else (0.1 if "firewood" in fuel_stacking else 0)

    primary_fuel = answers.get('primary_fuel')
    primary_fuel_score = 0.25 if primary_fuel == 'charcoal' else (0.1 if primary_fuel == "firewood" else 0)

    improved_cookstove_score = 0.1 if answers.get('improved_cookstove') == 'No' else 0
    
    burn_cookstove_score = 0.1 if answers.get('burn_cookstove') == 'No' else 0
    
    household_size = answers.get('household_size', 1)
    household_size_score = 0.15 * (0.1 if household_size == 1 else (0.5 if household_size == 2 else 1))

    meals_per_day = answers.get('meals_per_day', 1)
    meals_per_day_score = 0.2 * (0.1 if meals_per_day == 1 else (0.5 if meals_per_day == 2 else 1))
    
    overall_score = fuel_stacking_score + primary_fuel_score + improved_cookstove_score + burn_cookstove_score + household_size_score + meals_per_day_score

    return format(overall_score, ".0%")