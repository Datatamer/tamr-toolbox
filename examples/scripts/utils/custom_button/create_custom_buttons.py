"""
Example script for creating and saving custom button yaml files
"""
import tamr_toolbox as tbox
import argparse


def main(output_dir):

    # Create button dicts
    example_redirect_button1 = tbox.utils.custom_button.create_redirect_dict(
        button_id="button_1",
        button_text="Google",
        page_names=["Schema Mapping:Dashboard", "Mastering:Dashboard"],
        redirect_url="https://www.google.com",
        open_in_new_tab=True,
    )

    # Create button dicts
    example_redirect_button2 = tbox.utils.custom_button.create_redirect_dict(
        button_id="button_2",
        button_text="Yahoo",
        page_names=["Schema Mapping:Dashboard", "Mastering:Dashboard"],
        redirect_url="https://www.yahoo.com",
        open_in_new_tab=True,
    )

    example_post_button = tbox.utils.custom_button.create_post_dict(
        button_id="button_2",
        button_text="Export Project",
        page_names=["Mastering:Clusters"],
        post_url="https://example-location.tamr.com/python/api/export",
        post_body_keys=["projectId"],
        success_message="Project export sucessful",
        fail_message="Project export failed",
        display_response=True,
    )

    button_list = [example_redirect_button1, example_post_button]

    # Save a button individually
    tbox.utils.custom_button.save_button_as_yaml(
        button_name="yahoo", button_dict=example_redirect_button2, output_dir=output_dir
    )

    # Or save multiple buttons as an extension yaml file
    tbox.utils.custom_button.create_button_extension(
        extension_name="Extension 1", buttons=button_list, output_dir=output_dir
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
