FROM scratch
ADD bin/server /server
ENTRYPOINT ["/server"]