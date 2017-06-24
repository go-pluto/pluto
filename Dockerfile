FROM alpine

ADD pluto /bin/

ENTRYPOINT ["/usr/bin/pluto"]
CMD ["-config", "/etc/pluto.toml"] # default location which can be easily overwritten from the outside
