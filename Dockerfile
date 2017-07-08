FROM alpine:3.4

ADD ./pluto /bin/

ENTRYPOINT ["/bin/pluto"]
