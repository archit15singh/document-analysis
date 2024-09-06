from pathlib import Path
import pdfplumber
import json
import unicodedata  # Importing the library for Unicode normalization
import multiprocessing
import os


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

        return {
            "page_num": page_num + 1,
            "text": clean_page_text,  # Use cleaned text
            "images": images_data,
        }


def extract_images_and_text_from_pdf_parallel(pdf_path, resolution=300):
    """Main function to handle parallel processing of PDF pages."""
    with pdfplumber.open(pdf_path) as pdf:
        num_pages = len(pdf.pages)

    num_cpus = os.cpu_count()  # Get the number of available CPU cores

    # Prepare multiprocessing pool with number of CPUs
    with multiprocessing.Pool(processes=num_cpus) as pool:
        tasks = [
            pool.apply_async(process_page, (pdf_path, page_num, resolution))
            for page_num in range(num_pages)
        ]

        # Collect the results as they are processed
        pages_data = [task.get() for task in tasks]

    return pages_data


def process_pdf(pdf_file, resolution=300):
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

    # Step 1: Extract images, text, and metadata from PDF using parallel processing
    pages_data = extract_images_and_text_from_pdf_parallel(
        pdf_file, resolution=resolution
    )

    # Step 2: Attach metadata and text to response
    response["pages"] = pages_data

    return response


if __name__ == "__main__":
    pdf_file = (
        "/Users/architsingh/Documents/projects/document-analysis/raw/Fanuc 31i.pdf"
    )

    # Process the PDF with higher resolution for images
    response_data = process_pdf(pdf_file, resolution=300)  # Set the DPI to 300

    # Save the response data to a JSON file for later use
    with open("response_data.json", "w") as f:
        json.dump(response_data, f, indent=4)

    print("Processing complete! Metadata, text, and images saved.")
