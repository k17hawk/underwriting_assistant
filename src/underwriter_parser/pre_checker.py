# pre_checker.py
import io
from pathlib import Path
from typing import Tuple, Optional
import PyPDF2
from PIL import Image
import fitz  # PyMuPDF
from underwriter_parser.entity.config import config

class FilePrecheck:
    """Synchronous file validation before any async processing."""
    
    MIN_SIZE_BYTES = 100
    MAX_SIZE_BYTES = config.storage.max_file_size_bytes
    
    # Supported image formats
    SUPPORTED_IMAGE_FORMATS = {'.webp', '.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.gif'}
    
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
        
        # Check if it's a PDF
        if file_content.startswith(b'%PDF'):
            return FilePrecheck._validate_pdf_content(file_content)
        
        # Try to convert image to PDF
        return FilePrecheck._convert_image_to_pdf(file_content)
    
    @staticmethod
    def _validate_pdf_content(file_content: bytes) -> Tuple[bool, Optional[str], Optional[bytes]]:
        """Validate PDF content."""
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            
            if pdf_reader.is_encrypted:
                return False, "PDF is password protected", None
            
            if len(pdf_reader.pages) == 0:
                return False, "PDF has no pages", None
            
            # Additional validation with PyMuPDF
            doc = fitz.open(stream=file_content, filetype="pdf")
            if doc.is_encrypted:
                return False, "PDF is password protected (PyMuPDF)", None
            doc.close()
            
            return True, None, file_content
            
        except Exception as e:
            return False, f"PDF validation failed: {str(e)}", None
    
    @staticmethod
    def _convert_image_to_pdf(file_content: bytes) -> Tuple[bool, Optional[str], Optional[bytes]]:
        """Convert image to PDF."""
        try:
            # Try to open as image
            image = Image.open(io.BytesIO(file_content))
            
            # Handle RGBA images (convert to RGB)
            if image.mode == 'RGBA':
                image = image.convert('RGB')
            elif image.mode == 'P':  # Palette mode
                image = image.convert('RGB')
            
            # Create PDF in memory
            pdf_buffer = io.BytesIO()
            image.save(pdf_buffer, 'PDF', resolution=100.0)
            pdf_content = pdf_buffer.getvalue()
            
            # Validate the generated PDF
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            if len(pdf_reader.pages) == 0:
                return False, "Generated PDF has no pages", None
            
            print(f"✅ Converted image to PDF ({len(pdf_content)} bytes)")
            return True, None, pdf_content
            
        except Exception as e:
            return False, f"File is not a valid PDF or supported image format: {str(e)}", None
    
    @staticmethod
    def is_image_file(filename: str) -> bool:
        """Check if file is a supported image format."""
        ext = Path(filename).suffix.lower()
        return ext in FilePrecheck.SUPPORTED_IMAGE_FORMATS