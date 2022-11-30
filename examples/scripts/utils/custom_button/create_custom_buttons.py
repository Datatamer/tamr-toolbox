"""
Example script for creating and saving custom button yaml files
"""
import tamr_toolbox as tbox
import argparse
import logging

LOGGER = logging.getLogger(__name__)


def main(output_dir):

    LOGGER.info("Creating button yaml files...")
    tbox.utils.custom_button.create_redirect_button(
        button_id="redirect_button_1",
        button_text="Google",
        page_names=["Schema Mapping:Dashboard", "Mastering:Dashboard"],
        redirect_url="https://www.google.com",
        open_in_new_tab=True,
        output_dir=output_dir,
        button_name="redirect_button_1",
    )

    tbox.utils.custom_button.create_redirect_button(
        button_id="redirect_button_2",
        button_text="Yahoo",
        page_names=["Schema Mapping:Dashboard", "Mastering:Dashboard"],
        redirect_url="https://www.yahoo.com",
        open_in_new_tab=True,
        output_dir=output_dir,
        button_name="redirect_button_2",
    )

    tbox.utils.custom_button.create_post_button(
        button_id="post_button_1",
        button_text="Export Project",
        page_names=["Mastering:Clusters"],
        post_url="https://example-location.tamr.com/python/api/export",
        post_body_keys=["projectId"],
        success_message="Project export sucessful",
        fail_message="Project export failed",
        display_response=True,
        output_dir=output_dir,
        button_name="post_button_1",
    )

    # Register a button individually
    LOGGER.info(f"Registering individual button")
    tbox.utils.custom_button.register_button(
        button="/home/ubuntu/tamr/redirect_button_1.yaml", tamr_install_dir="/home/ubuntu"
    )

    # Group multiple buttons into an extension yaml & register it
    button_list = [f"{output_dir}/redirect_button_2.yaml", f"{output_dir}/post_button_1.yaml"]
    LOGGER.info("Registering extension")
    tbox.utils.custom_button.create_button_extension(
        extension_name="extension_1", buttons=button_list, output_dir=output_dir
    )


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    parser.add_argument("--output_dir", help="path to output directory", required=True)

    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/conf/project.config.yaml"
    )
    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])
    # Run the main function

    main(args.output_dir)
