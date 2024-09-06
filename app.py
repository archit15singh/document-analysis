import os
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
    with pdfplumber.open(pdf_path) as pdf:
        # Get title from document metadata
        title = pdf.metadata.get("Title", "Unknown Title")

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

                # Save extracted image
                image_folder = "./images"
                os.makedirs(image_folder, exist_ok=True)
                image_path = f"{image_folder}/extracted_image_page_{page_num + 1}_img_{image_idx}.png"
                extracted_image.save(image_path)

                # Add image data to list
                images_data.append({"image_idx": image_idx, "image_path": image_path})

            yield {
                "page_num": page_num + 1,
                "title": title,
                "text": clean_page_text,  # Use cleaned text
                "images": images_data,
            }


def process_pdf(pdf_file, resolution=300):  # Added resolution parameter
    response = {"pdf_path": pdf_file, "pages": []}

    # Step 1: Extract images, text, and metadata from PDF
    for page_data in extract_images_and_text_from_pdf(pdf_file, resolution=resolution):
        # Step 2: Attach metadata and text to response
        response["pages"].append(
            {
                "page_num": page_data["page_num"],
                "title": page_data["title"],
                "text": page_data["text"],
                "images": page_data["images"],
            }
        )

    return response


if __name__ == "__main__":
    pdf_file = "/Users/architsingh/Documents/projects/document-analysis/raw/backup procedure for laser cutting machines with 31i controller_versie1_1.pdf"

    # Process the PDF with higher resolution for images
    response_data = process_pdf(pdf_file, resolution=300)  # Set the DPI to 300

    # Save the response data to a JSON file for later use
    with open("response_data.json", "w") as f:
        json.dump(response_data, f, indent=4)

    print("Processing complete! Metadata, text, and images saved.")
