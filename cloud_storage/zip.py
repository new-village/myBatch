import zipfile
import os
import sys
import logging

def zip_folder(folder_path, output_zip):
    """Compresses the specified folder into a ZIP file.

    Args:
        folder_path (str): The path to the folder to compress.
        output_zip (str): The path where the ZIP file will be saved.
    """
    logging.info(f"Starting compression: '{folder_path}' into '{output_zip}'.")
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Recursively traverse all files and directories in the specified folder
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Create the full file path
                file_path = os.path.join(root, file)
                # Preserve folder structure while adding file to ZIP
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
                logging.debug(f"Added: '{file_path}' as '{arcname}' to the archive.")
    logging.info(f"Compression completed: '{output_zip}' has been created.")

def main():
    """Main function to parse arguments and initiate folder compression."""
    if len(sys.argv) != 2 or sys.argv[1] not in ['horse', 'race']:
        logging.error("Usage: python script.py [horse|race]")
        sys.exit(1)
    folder_name = sys.argv[1]
    folder_to_zip = f'/data/{folder_name}/'
    output_zip_file = f'/data/{folder_name}.zip'

    if not os.path.exists(folder_to_zip):
        logging.error(f"Error: Folder '{folder_to_zip}' does not exist.")
        sys.exit(1)

    zip_folder(folder_to_zip, output_zip_file)

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler("zip_script.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    main()
