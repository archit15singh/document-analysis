from pathlib import Path
import pdfplumber
import json
import unicodedata  # Importing the library for Unicode normalization


def clean_text(text):
    if text:
        # Normalize unicode characters and remove problematic ones
        normalized_text = unicodedata.normalize("NFKD", text)
        # Replace any remaining non-ASCII characters with a placeholder or remove them
        clean_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return clean_text
    return text


def extract_images_and_text_from_pdf(
    pdf_path, resolution=300
):  # Added resolution parameter for higher DPI
    file_name = Path(pdf_path).stem  # Extract file name without extension
    file_extension = Path(pdf_path).suffix.lstrip(
        "."
    )  # Extract the extension without the leading dot

    with pdfplumber.open(pdf_path) as pdf:

        for page_num, page in enumerate(pdf.pages):
            print(f"Processing Page {page_num + 1}")
            images_data = []
            text = page.extract_text()
            clean_page_text = clean_text(text)  # Clean the extracted text

            for image_idx, image in enumerate(page.images):
                image_bbox = (image["x0"], image["top"], image["x1"], image["bottom"])
                # Extract the image using the image bounding box
                extracted_image = (
                    page.within_bbox(image_bbox)
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

            yield {
                "page_num": page_num + 1,
                "text": clean_page_text,  # Use cleaned text
                "images": images_data,
            }, file_name, file_extension


def process_pdf(pdf_file, resolution=600):  # Added resolution parameter
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

    # Step 1: Extract images, text, and metadata from PDF
    for page_data, file_name, file_extension in extract_images_and_text_from_pdf(
        pdf_file, resolution=resolution
    ):
        # Step 2: Attach metadata and text to response
        response["pages"].append(
            {
                "page_num": page_data["page_num"],  # Only page_num
                "text": page_data["text"],  # Add text
                "images": page_data["images"],  # List of images with index and path
            }
        )

    return response


if __name__ == "__main__":
    pdf_file = "/Users/architsingh/Documents/projects/document-analysis/raw/backup procedure for laser cutting machines with 31i controller_versie1_1.pdf"

    # Process the PDF with higher resolution for images
    response_data = process_pdf(pdf_file, resolution=600)  # Set the DPI to 300

    # Save the response data to a JSON file for later use
    with open("response_data.json", "w") as f:
        json.dump(response_data, f, indent=4)

    print("Processing complete! Metadata, text, and images saved.")
