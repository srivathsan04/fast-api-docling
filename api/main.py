from fastapi import FastAPI, UploadFile, HTTPException
from pathlib import Path
import tempfile
import json
import logging
import pandas as pd
from docling.document_converter import DocumentConverter, InputFormat, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions

app = FastAPI()

def process_pdf(pdf_path: Path) -> list:
    """Process the PDF and return JSON list of rows."""
    logging.basicConfig(level=logging.INFO)
    
    # Set up pipeline options to disable OCR only (add more if needed)
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = False
    
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )
    
    # Only process the first 2 pages (1 and 2)
    conv_res = doc_converter.convert(pdf_path, page_range=(1, 2))
    
    canonical_headers = None
    all_rows = []
    for table in conv_res.document.tables:
        table_df: pd.DataFrame = table.export_to_dataframe()
        if table_df.empty:
            continue
        # If columns are all integers, treat the first row as header
        if all(isinstance(col, int) for col in table_df.columns):
            table_df.columns = table_df.iloc[0]
            table_df = table_df[1:].reset_index(drop=True)
        # Set canonical headers from the first table
        if canonical_headers is None:
            canonical_headers = list(table_df.columns)
        # Map columns by position if headers don't match
        if list(table_df.columns) != canonical_headers:
            table_df.columns = canonical_headers[:len(table_df.columns)]
        # Only keep the desired columns (exclude Chq./Ref.No. and Value Dt)
        date_col = next((col for col in table_df.columns if str(col).strip().lower() == 'date'), None)
        desc_col = next((col for col in table_df.columns if str(col).strip().lower() in ['narration', 'description']), None)
        withdraw_col = next((col for col in table_df.columns if 'withdrawal' in str(col).lower()), None)
        deposit_col = next((col for col in table_df.columns if 'deposit' in str(col).lower()), None)
        balance_col = next((col for col in table_df.columns if 'closing balance' in str(col).lower()), None)
        filtered_cols = [date_col, desc_col, withdraw_col, deposit_col, balance_col]
        filtered_cols = [col for col in filtered_cols if col and col in table_df.columns]
        if not filtered_cols:
            continue
        filtered_df = table_df[filtered_cols]
        # Rename columns for consistency
        rename_map = {}
        if date_col: rename_map[date_col] = 'Date'
        if desc_col: rename_map[desc_col] = 'Narration'
        if withdraw_col: rename_map[withdraw_col] = 'Withdrawal Amount'
        if deposit_col: rename_map[deposit_col] = 'Deposit Amount'
        if balance_col: rename_map[balance_col] = 'Closing Balance'
        filtered_df = filtered_df.rename(columns=rename_map)
        if not filtered_df.empty:
            all_rows.extend(filtered_df.to_dict(orient='records'))
    
    return all_rows

@app.post("/process-pdf")
async def process_pdf_endpoint(file: UploadFile):
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
        temp_file.write(await file.read())
        temp_path = Path(temp_file.name)
    
    try:
        # Process the PDF
        result = process_pdf(temp_path)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
    finally:
        # Clean up temp file
        temp_path.unlink()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)