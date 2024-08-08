_ENV = setmetatable({}, {
  __index = _G,
  __newindex = function() error("cannot set global variable", 2) end,
})

local _M = {}

local find = string.find
local gsub = string.gsub
local concat = table.concat
local unpack = table.unpack

local function quote_literal(s)
  return "'" .. gsub(s, "'", "''") .. "'"
end

local function quote_ident(s)
  return '"' .. gsub(s, "'", "''") .. '"'
end

local function assemble(args)
  local result = {}
  local param_counter = 0
  local inner_impl
  local function inner(...)
    local result = inner_impl(...)
    if find(result, "%$") then
      error("unexpected dollar")
    end
    return result
  end
  function inner_impl(args)
    local tp = type(args)
    if tp == "string" then
      return args
    end
    if tp == "number" then
      return tostring(args)
    end
    local used_param_idxs = {}
    local ident_idx = 0
    local templ = args[1]
    if find(templ, "\0") then
      error("unexpected null byte")
    end
    templ = gsub(templ, "$([0-9A-Za-z_]*)$([^$]*)($?)", function(ident, sep, d)
      if d == "" then
        error("unterminated dollar sequence")
      end
      if ident == "" then
        ident_idx = ident_idx+1
        ident = ident_idx
      else
        ident = tonumber(ident) or ident
      end
      if sep == "" then
        return inner(args[ident+1])
      else
        local parts = {}
        for idx, element in ipairs(args[ident+1]) do
          parts[idx] = inner(element)
        end
        return concat(parts, sep)
      end
    end)
    templ = gsub(templ, "$([0-9A-Za-z_]*)", function(ident)
      if ident == "" then
        ident_idx = ident_idx+1
        ident = ident_idx
      else
        ident = tonumber(ident) or ident
      end
      local param_idx = used_param_idxs[ident]
      if not param_idx then
        param_counter = param_counter + 1
        param_idx = param_counter
        result[param_idx+1] = args[ident+1]
        result.param_count = param_idx
        used_param_idxs[ident] = param_idx
      end
      return "\0" .. param_idx
    end)
    return templ
  end
  result[1] = gsub(inner(args), "\0", "$")
  return result
end
_M.assemble = assemble

local function add(tbl, key, value)
  local subtbl = tbl[key]
  if not subtbl then
    subtbl = {}
    tbl[key] = subtbl
  end
  subtbl[#subtbl+1] = value
end

_M.selector_prototype = {}

function _M.new()
  return setmetatable({}, _M.selector_metatbl)
end

function _M.selector_prototype:add_with(alias, subselect)
  add(self, "_with", {'$$$ AS ($$$)', quote_ident(alias), subselect})
  return self
end

function _M.selector_prototype:distinct()
  if self._distinct_on then
    error("cannot combine DISTINCT with DISTINCT ON")
  end
  self._distinct = true
  return self
end

function _M.selector_prototype:add_distinct_on(ident)
  if self._distinct then
    error("cannot combine DISTINCT with DISTINCT ON")
  end
  add(self, "_distinct_on", ident)
  return self
end

function _M.selector_prototype:add_field(expr, alias)
  if alias then
    add(self, "_fields", {'$$$ AS $$$', expr, quote_ident(alias)})
  else
    add(self, "_fields", expr)
  end
  return self
end

function _M.selector_prototype:reset_fields()
  self._fields = nil
  return self
end

function _M.selector_prototype:add_from(expr, alias, cond)
  local first = not self._from
  if not first then
    if cond then
      add(self, "_from", 'INNER JOIN')
    else
      add(self, "_from", 'CROSS JOIN')
    end
  end
  if getmetatable(expr) == _M.selector_metatbl then
    if alias then
      add(
        self, "_from", {'($$$) AS $$$', expr:build_table(), quote_ident(alias)}
      )
    else
      add(self, "_from", {'($$$) AS "subquery"', expr:build_table()})
    end
  else
    if alias then
      add(self, "_from", {'$$$ AS $$$', expr, quote_ident(alias)})
    else
      add(self, "_from", expr)
    end
  end
  if cond then
    if first then
      self:add_where(cond)
    else
      add(self, "_from", 'ON')
      add(self, "_from", cond)
    end
  end
  return self
end

function _M.selector_prototype:left_join(expr, alias, cond)
  local first = not self._from
  if not first then
    add(self, "_from", 'LEFT OUTER JOIN')
  end
  if getmetatable(expr) == _M.selector_metatbl then
    if alias then
      add(
        self, "_from", {'($$$) AS $$$', expr:build_table(), quote_ident(alias)}
      )
    else
      add(self, "_from", {'($$$) AS "subquery"', expr:build()})
    end
  else
    if alias then
      add(self, "_from", {'$$$ AS "$$$"', expr, quote_ident(alias)})
    else
      add(self, "_from", expr)
    end
  end
  if first then
    if cond then
      self:add_where(cond)
    end
  else
    add(self, "_from", 'ON')
    add(self, "_from", cond or 'TRUE')
  end
  return self
end

function _M.selector_prototype:add_where(expr)
  add(self, "_where", expr)
  return self
end

function _M.selector_prototype:add_group_by(expr)
  add(self, "_group_by", expr)
  return self
end

function _M.selector_prototype:add_having(expr)
  add(self, "_having", expr)
  return self
end

function _M.selector_prototype:add_combine(expr)
  add(self, "_combine", expr)
  return self
end

function _M.selector_prototype:union(subselect)
  self:add_combine('UNION')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:union_all(subselect)
  self:add_combine('UNION ALL')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:intersect(subselect)
  self:add_combine('INTERSECT')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:intersect_all(subselect)
  self:add_combine('INTERSECT ALL')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:except(subselect)
  self:add_combine('EXCEPT')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:except_all(subselect)
  self:add_combine('EXCEPT ALL')
  self:add_combine(subselect)
  return self
end

function _M.selector_prototype:add_order_by(expr)
  add(self, "_order_by", expr)
  return self
end

function _M.selector_prototype:limit(count)
  self._limit = count
  return self
end

function _M.selector_prototype:offset(count)
  self._offset = count
  return self
end

function _M.selector_prototype:for_share()
  self._read_lock_all = true
  return self
end

function _M.selector_prototype:for_share_of(expr)
  add(self, "_read_lock", expr)
  return self
end

function _M.selector_prototype:for_update()
  self._write_lock_all = true
  return self
end

function _M.selector_prototype:for_update_of(expr)
  add(self, "_write_lock", expr)
  return self
end

function _M.selector_prototype:build_table()
  local parts = {}
  if self._with then
    parts[#parts+1] = {"WITH RECURSIVE $$, $", self._with}
  end
  parts[#parts+1] = "SELECT"
  if self._distinct then
    parts[#parts+1] = "DISTINCT"
  elseif self._distinct_on then
    parts[#parts+1] = {"DISTINCT ON ($$, $)", self._distinct_on}
  end
  if self._fields then
    parts[#parts+1] = {'$$, $', self._fields}
  else
    parts[#parts+1] = '*'
  end
  if self._from then
    parts[#parts+1] = {'FROM $$ $', self._from}
  end
  if self._where then
    parts[#parts+1] = {'WHERE $$ AND $', self._where}
  end
  if self._group_by then
    parts[#parts+1] = {'GROUP BY $$, $', self._group_by}
  end
  if self._having then
    parts[#parts+1] = {'HAVING $$ AND $', self._having}
  end
  if self._combine then
    for idx, part in ipairs(self._combine) do
      parts[#parts+1] = part
    end
  end
  if self._order_by then
    parts[#parts+1] = {'ORDER BY $$, $', self._order_by}
  end
  if self._limit then
    parts[#parts+1] = {'LIMIT $', self._limit}
  end
  if self._offset then
    parts[#parts+1] = {'OFFSET $', self._offset}
  end
  if self._write_lock_all then
    parts[#parts+1] = 'FOR UPDATE'
  else
    if self._read_lock_all then
      parts[#parts+1] = 'FOR SHARE'
    elseif self._read_lock then
      parts[#parts+1] = {'FOR SHARE OF $$, $', self._read_lock}
    end
    if self._write_lock then
      parts[#parts+1] = {'FOR UPDATE OF $$, $', self._write_lock}
    end
  end
  return assemble{"$$ $", parts}
end

function _M.selector_prototype:build()
  local tbl = self:build_table()
  return unpack(tbl, 1, tbl.param_count + 1)
end

function _M.selector_prototype:build_string()
  local tbl = self:build_table()
  return (gsub(tbl[1], "$([0-9]+)", function(param_idx)
    return quote_literal(tostring(tbl[tonumber(param_idx+1)]))
  end))
end

_M.selector_metatbl = {
  __index = _M.selector_prototype,
  __tostring = _M.selector_prototype.build_string,
}

return _M
