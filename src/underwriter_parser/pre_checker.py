# precheck.py
import os
import io
from pathlib import Path
from typing import Tuple, Optional
import PyPDF2
from PIL import Image
import fitz  # PyMuPDF
from underwriter_parser.entity import config

class FilePrecheck:
    """Synchronous file validation before any async processing."""
    
    MIN_SIZE_BYTES = 100
    MAX_SIZE_BYTES = config.storage.max_file_size_bytes
    
    @staticmethod
    def validate_pdf(file_content: bytes) -> Tuple[bool, Optional[str], Optional[bytes]]:
        """
        Synchronous pre-check.
        Returns: (is_valid, error_message, processed_content)
        """
        # Size check
        if len(file_content) < FilePrecheck.MIN_SIZE_BYTES:
            return False, f"File too small: {len(file_content)} bytes", None
        
        if len(file_content) > FilePrecheck.MAX_SIZE_BYTES:
            return False, f"File too large: {len(file_content)} bytes", None
        
        # PDF header check
        if not file_content.startswith(b'%PDF'):
            # Try to convert image to PDF
            try:
                image = Image.open(io.BytesIO(file_content))
                if image.mode == 'RGBA':
                    image = image.convert('RGB')
                
                pdf_buffer = io.BytesIO()
                image.save(pdf_buffer, 'PDF', resolution=100.0)
                file_content = pdf_buffer.getvalue()
                return True, None, file_content
            except Exception:
                return False, "File is not a valid PDF or image", None
        
        # Parse PDF for additional checks
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            
            # Check if password protected
            if pdf_reader.is_encrypted:
                return False, "PDF is password protected", None
            
            # Page count
            page_count = len(pdf_reader.pages)
            if page_count == 0:
                return False, "PDF has no pages", None
            
            # Additional validation with PyMuPDF for robustness
            doc = fitz.open(stream=file_content, filetype="pdf")
            if doc.is_encrypted:
                return False, "PDF is password protected (PyMuPDF)", None
            
            return True, None, file_content
            
        except Exception as e:
            return False, f"PDF validation failed: {str(e)}", None