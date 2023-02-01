local function main()
    local c = db:consumer()
    db:num_columns(2)
    db:column_type("int", 1)
    db:column_type("blob", 2)
    db:column_name("a", 1)
    db:column_name("b", 2)
    local i = 0
    while i < 1000 do
        local ev = c:get()
        if ev ~= nil then
            c:emit(ev.new)
            c:consume()
        else
            return
        end
        i = i + 1
    end
end
