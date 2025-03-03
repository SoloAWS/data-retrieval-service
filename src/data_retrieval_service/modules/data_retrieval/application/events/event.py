import traceback
from uuid import UUID
from ...domain.events import ImageUploadFailed

async def create_image_upload_failed_event(
    task_id: UUID,
    filename: str,
    error: Exception,
    source: str = None,
    format_str: str = None, 
    modality: str = None,
    region: str = None
) -> ImageUploadFailed:
    """
    Creates an ImageUploadFailed event with details about the error
    
    Args:
        task_id: ID of the task associated with the image upload
        filename: Name of the file that failed to upload
        error: Exception that occurred
        source: Source of the image (optional)
        format_str: Format of the image (optional)
        modality: Modality of the image (optional)
        region: Region of the image (optional)
        
    Returns:
        An ImageUploadFailed event
    """
    # Get the full stack trace as a string
    stack_trace = ''.join(traceback.format_tb(error.__traceback__))
    
    return ImageUploadFailed(
        task_id=task_id,
        filename=filename,
        error_message=str(error),
        source=source,
        format=format_str,
        modality=modality,
        region=region,
        stack_trace=stack_trace
    )