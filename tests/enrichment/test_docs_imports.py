import os
from importlib import reload


def test_docs_imports():
    # Set environment to include optional dependencies
    existing_var = os.getenv("TAMR_TOOLBOX_DOCS")
    os.environ["TAMR_TOOLBOX_DOCS"] = "1"

    from tamr_toolbox.enrichment import address_validation, translate
    from tamr_toolbox.enrichment.api_client import google_address_validate, google_translate

    reload(address_validation)
    reload(translate)
    reload(google_address_validate)
    reload(google_translate)

    # Reset to state before test started
    if existing_var:
        os.environ["TAMR_TOOLBOX_DOCS"] = existing_var
    else:
        os.environ.pop("TAMR_TOOLBOX_DOCS")
