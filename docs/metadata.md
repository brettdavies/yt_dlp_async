# Documentation for `metadata.py`

## Module Overview

The `metadata.py` module provides the `Metadata` class, which contains a dictionary mapping various representations of Major League Baseball (MLB) team names to their standard abbreviations. This mapping is essential for normalizing team names during data processing, ensuring consistency across the application.

### Key Component

- **Team Abbreviations Mapping**: A comprehensive dictionary that maps full team names, nicknames, and abbreviations to a standard three-letter abbreviation.

---

## Class and Attribute

### `Metadata`

Provides mappings from team names to their standard abbreviations.

#### Attributes

- **`team_abbreviations`** (`Dict[str, str]`): A class variable that is a dictionary mapping various forms of team names to their standard three-letter abbreviations.

---

## Example Usage

### Accessing Team Abbreviations

You can access the team abbreviations directly from the `Metadata` class without instantiating it, as `team_abbreviations` is a class variable.

```python
from metadata import Metadata

# Get the abbreviation for 'los angeles angels'
team_abbr = Metadata.team_abbreviations['los angeles angels']
print(team_abbr)  # Output: 'ana'

# Normalize a team name
team_name = 'Yankees'
normalized_team = Metadata.team_abbreviations.get(team_name.lower(), 'unknown')
print(normalized_team)  # Output: 'nya'
```

---

## Understanding the Mapping

The `team_abbreviations` dictionary includes:

- **Full Team Names**: e.g., `'boston red sox', 'atlanta braves'`
- **Nicknames**: e.g., `'red sox'`, `'braves'`
- **City Names**: e.g., `'boston'`, `'atlanta'`
- **Abbreviations**: e.g., `'bos'`, `'atl'`
- **Historical Team Names**: e.g., `'montreal expos'`
- **Alternative Names**: e.g., `'cal'` for `'anaheim angels'`

This comprehensive mapping ensures that different representations of team names are correctly mapped to a standard abbreviation, which is crucial for data consistency.

---

## Additional Notes

- **Case Sensitivity**: The keys in `team_abbreviations` are in lowercase. When accessing the dictionary, ensure that the team name is converted to lowercase to match the keys.

  ```python
  team_name = 'Yankees'
  normalized_team = Metadata.team_abbreviations.get(team_name.lower(), 'unknown')
  ```

- **Extensibility**: You can extend the `team_abbreviations` dictionary by adding new mappings if needed.

  ```python
  Metadata.team_abbreviations['new team name'] = 'new_abbr'
  ```

- **Usage in Data Processing**: This mapping is often used in conjunction with data parsing functions to normalize team names extracted from various data sources like text, JSON, or CSV files.

---

## Conclusion

The `metadata.py` module provides a vital utility for normalizing team names within the application. By using the `Metadata` class and its `team_abbreviations` attribute, developers can ensure consistency in team name representation, which is essential for accurate data processing and analysis.

---

**Note**: Ensure that when you use the `Metadata` class, the team names you provide as keys are in lowercase to match the dictionary keys. This practice helps avoid key mismatches due to case sensitivity.
