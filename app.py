from pathlib import Path
import pdfplumber
import json
import unicodedata
import multiprocessing
import os
from loguru import logger  # Import loguru for logging
from time import time  # For timing the operations
import argparse  # For command-line arguments


# Configure loguru to log to a file and the console
logger.add("process_pdf.log", rotation="1 MB")  # Log rotation when the log reaches 1 MB


def clean_text(text):
    if text:
        # Normalize unicode characters and remove problematic ones
        normalized_text = unicodedata.normalize("NFKD", text)
        # Replace any remaining non-ASCII characters with a placeholder or remove them
        clean_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return clean_text
    return text


def process_page(pdf_path, page_num, resolution=300):
    """Function to process a single page and return its extracted images and text."""
    start_time = time()  # Start timing the page processing
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page_data = pdf.pages[page_num]
            images_data = []
            text = page_data.extract_text()
            clean_page_text = clean_text(text)  # Clean the extracted text

            for image_idx, image in enumerate(page_data.images):
                image_bbox = (image["x0"], image["top"], image["x1"], image["bottom"])
                # Extract the image using the image bounding box
                extracted_image = (
                    page_data.within_bbox(image_bbox)
                    .to_image(resolution=resolution)
                    .original
                )

                # Save extracted image using pathlib
                image_folder = Path("./images")
                image_folder.mkdir(parents=True, exist_ok=True)
                image_path = (
                    image_folder
                    / f"extracted_image_page_{page_num + 1}_img_{image_idx}.png"
                )
                extracted_image.save(image_path)

                # Add image data to list
                images_data.append({"index": image_idx, "path": str(image_path)})

            logger.info(f"Processed page {page_num + 1} successfully.")
            end_time = time()  # End timing
            logger.info(
                f"Page {page_num + 1} processed in {end_time - start_time:.2f} seconds."
            )

            return {
                "page_num": page_num + 1,
                "text": clean_page_text,  # Use cleaned text
                "images": images_data,
            }
    except Exception as e:
        logger.error(f"Error processing page {page_num + 1}: {e}")
        return {
            "page_num": page_num + 1,
            "text": None,
            "images": [],
            "error": str(e),
        }


def extract_images_and_text_from_pdf_parallel(pdf_path, resolution=300):
    """Main function to handle parallel processing of PDF pages."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)
            logger.info(f"Started processing {num_pages} pages from {pdf_path}.")

        num_cpus = os.cpu_count()  # Get the number of available CPU cores
        logger.info(f"Using {num_cpus} CPU cores for parallel processing.")

        start_time = time()  # Start timing the parallel processing
        # Prepare multiprocessing pool with number of CPUs
        with multiprocessing.Pool(processes=num_cpus) as pool:
            tasks = [
                pool.apply_async(process_page, (pdf_path, page_num, resolution))
                for page_num in range(num_pages)
            ]

            # Collect the results as they are processed
            pages_data = [task.get() for task in tasks]

        end_time = time()  # End timing the parallel processing
        logger.info(
            f"Successfully processed all {num_pages} pages in {end_time - start_time:.2f} seconds."
        )
        return pages_data

    except Exception as e:
        logger.critical(f"Failed to process PDF {pdf_path}: {e}")
        raise


def process_pdf(pdf_file, resolution=600):
    file_name = Path(pdf_file).stem  # Extract file name without extension
    file_extension = Path(pdf_file).suffix.lstrip(
        "."
    )  # Extract the extension without the leading dot

    response = {
        "pdf_path": pdf_file,
        "file_name": file_name,  # Store file name without extension
        "extension": file_extension,  # Store the extension (e.g., 'pdf', 'md')
        "pages": [],
    }

    try:
        logger.info(f"Beginning PDF processing: {pdf_file}")
        pdf_start_time = time()  # Start timing the entire PDF processing

        # Step 1: Extract images, text, and metadata from PDF using parallel processing
        pages_data = extract_images_and_text_from_pdf_parallel(
            pdf_file, resolution=resolution
        )

        # Step 2: Attach metadata and text to response
        response["pages"] = pages_data

        pdf_end_time = time()  # End timing the entire PDF processing
        logger.info(
            f"Completed PDF processing: {pdf_file} in {pdf_end_time - pdf_start_time:.2f} seconds."
        )

    except Exception as e:
        logger.error(f"Failed to process PDF {pdf_file}: {e}")
        return None

    return response


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Process a PDF to extract images and text."
    )
    parser.add_argument(
        "pdf_file", type=str, help="The path to the PDF file to be processed."
    )

    # Parse the arguments
    args = parser.parse_args()
    pdf_file = args.pdf_file

    logger.info(f"Starting to process {pdf_file}...")

    try:
        start_time = time()  # Start timing the main process

        # Process the PDF with higher resolution for images
        response_data = process_pdf(pdf_file, resolution=600)  # Set the DPI to 600

        if response_data:
            # Save the response data to a JSON file for later use
            with open("response_data.json", "w") as f:
                json.dump(response_data, f, indent=4)
            logger.info("Processing complete! Metadata, text, and images saved.")
        else:
            logger.error("Processing failed. No data was saved.")

        end_time = time()  # End timing the main process
        logger.info(f"Total time taken: {end_time - start_time:.2f} seconds.")

    except Exception as e:
        logger.critical(f"Critical error in main process: {e}")
