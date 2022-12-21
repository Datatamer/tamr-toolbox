# Translation

This module requires an optional dependency. See [Installation](../../installation.md) for details.

## Translate

 ```eval_rst
 .. automodule:: tamr_toolbox.enrichment.translate

 ```

## Dictionary
Toolbox translation dictionaries have the following general format:
```
TranslationDictionary(
    standardized_phrase="cheddar cheese",
    translated_phrase="fromage cheddar",
    detected_language="en",
    original_phrases={"cheddar cheese"},
)
``` 
<br />

Toolbox translation dictionaries have the following format when loaded in memory to be able to 
access each dictionary by their standardized phrase:
```
{
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese",
        translated_phrase="fromage cheddar",
        detected_language="en",
        original_phrases={"cheddar cheese"},
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef",
        translated_phrase="boeuf haché",
        detected_language="en",
        original_phrases={"ground beef"},
    ),
}
```
<br />

When loaded on Tamr, translation dictionaries are source dataset with "standardized_phrase" as 
the primary key and the following attributes:
- "translated_phrase"
- "detected_language"
- "original_phrases"
<br />


 ```eval_rst
 .. automodule:: tamr_toolbox.enrichment.dictionary

 ```
