FROM alpine

ADD ./pluto /usr/bin/pluto

ENTRYPOINT ["/usr/bin/pluto"]
CMD ["-config", "/etc/pluto.toml"] # default location which can be easily overwritten from the outside
