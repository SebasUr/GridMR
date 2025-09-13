# Simple seeder image that bakes in repo testdata into /seed
FROM busybox:1.36
WORKDIR /
# Copy testdata directory from the build context into the image
COPY testdata /seed
# Default command is overridden by docker-compose
CMD ["sh", "-c", "ls -l /seed && echo 'Ready'"]
