FROM centurylink/ca-certs

ADD ./pluto /bin/

ENTRYPOINT ["/bin/pluto"]
