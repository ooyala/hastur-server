#!/bin/bash

JSON="{"
  JSON="${JSON}\"status\":\"OK\","
  JSON="${JSON}\"exit\":0,"
  JSON="${JSON}\"message\":\"bash extended plugin works fine\","
  JSON="${JSON}\"tags\":[\"version_0.1\",\"bash\",\"hastur\"],"
  JSON="${JSON}\"stats\":["
    JSON="${JSON}{\"runtime\":0.0,\"units\":\"s\"}"
  JSON="${JSON}]" # end of stats array
JSON="${JSON}}" # end of dict

echo $JSON
exit 0

