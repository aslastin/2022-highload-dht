wrk.method = "PUT"

counter = 1000000
request = function()
    wrk.body = string.rep("MiyLittlePony", 1000) .. counter
    path = "/v0/entity?id=" .. counter
    counter = counter + 1
    return wrk.format(nil, path)
end
