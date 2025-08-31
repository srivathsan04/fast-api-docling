from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
from docling.document_converter import DocumentConverter, InputFormat, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
import pandas as pd
import tempfile
import shutil
import logging

app = FastAPI()

def process_pdf(pdf_path: Path) -> list:
    """
    Process the PDF and return a list of dicts for all tables found on the first two pages,
    regardless of header names.
    """
    logging.basicConfig(level=logging.INFO)
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )
    conv_res = doc_converter.convert(pdf_path, page_range=(1, 2))
    all_rows = []
    for table in conv_res.document.tables:
        table_df: pd.DataFrame = table.export_to_dataframe()
        if not table_df.empty:
            # Convert each row to dict, using whatever headers are present
            all_rows.extend(table_df.to_dict(orient="records"))
    return all_rows

@app.post("/process-pdf")
async def process_pdf_endpoint(file: UploadFile):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="File must be a PDF")
    # Save uploaded file to a temp location
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)
    try:
        rows = process_pdf(tmp_path)
        return {"data": rows}
    finally:
        tmp_path.unlink(missing_ok=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)