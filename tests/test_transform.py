from src.utils.s3_utils import upload_bytes

upload_bytes(
    b"hello",
    bucket="vnstock-project",
    key="test/hello.txt",
    region="ap-southeast-1"
)