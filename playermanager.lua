--[[

Player management

Handles new player registry and player groups.

]]--

local db = ...

local u = pmutils

--[[
Random id generator, adapted from -- --
https://gist.github.com/haggen/2fd643ea9a261fea2094#gistcomment-2339900 -- --
--                              --
Generate random hex strings as player uuids -- --
]]                              --
local charset = {}  do -- [0-9a-f]
    for c = 48, 57  do table.insert(charset, string.char(c)) end
    for c = 97, 102 do table.insert(charset, string.char(c)) end
end

local function random_string(length)
    if not length or length <= 0 then return '' end
    math.randomseed(os.clock()^5)
    return random_string(length - 1) .. charset[math.random(1, #charset)]
end

--[[ player management proper ]]--

local function generate_id()
   return random_string(16)
end

local QUERY_REGISTER_PLAYER = [[
  INSERT INTO player (id, name, join_date)
  VALUES (?, ?, CURRENT_TIMESTAMP)
  ON CONFLICT DO NOTHING
]]

function pm.register_player(player_name)
   local player_id = generate_id()
   return assert(u.prepare(db, QUERY_REGISTER_PLAYER, player_id, player_name))
end

local QUERY_GET_PLAYER_BY_NAME = [[
  SELECT * FROM player WHERE player.name = ?
]]

function pm.get_player_by_name(player_name)
   local cur = u.prepare(db, QUERY_GET_PLAYER_BY_NAME, player_name)
   if cur then
      return cur:fetch({}, "a")
   else
      return nil
   end
end

local QUERY_GET_PLAYER_BY_ID = [[
  SELECT * FROM player WHERE player.id = ?
]]

function pm.get_player_by_id(player_id)
   local cur = u.prepare(db, QUERY_GET_PLAYER_BY_ID, player_id)
   if cur then
      return cur:fetch({}, "a")
   else
      return nil
   end
end

-- pm.register_player("Garfunel")

--[[ GROUPS ]]--

local QUERY_REGISTER_GROUP = [[
  INSERT INTO ctgroup (id, name, creation_date)
  VALUES (?, ?, CURRENT_TIMESTAMP)
  ON CONFLICT DO NOTHING
]]

function pm.register_group(ctgroup_name)
   local ctgroup_id = generate_id()
   return assert(u.prepare(db, QUERY_REGISTER_GROUP, ctgroup_id, ctgroup_name))
end

local QUERY_GET_GROUP_BY_NAME = [[
  SELECT * FROM ctgroup WHERE ctgroup.name = ?
]]

function pm.get_group_by_name(ctgroup_name)
   local cur = u.prepare(db, QUERY_GET_GROUP_BY_NAME, ctgroup_name)
   if cur then
      return cur:fetch({}, "a")
   else
      return nil
   end
end

local QUERY_GET_GROUP_BY_ID = [[
  SELECT * FROM ctgroup WHERE ctgroup.id = ?
]]

function pm.get_group_by_id(ctgroup_id)
   local cur = u.prepare(db, QUERY_GET_GROUP_BY_ID, ctgroup_id)
   if cur then
      return cur:fetch({}, "a")
   else
      return nil
   end
end

local QUERY_DELETE_GROUP = [[
  DELETE FROM ctgroup
  WHERE ctgroup.id = ?
]]

function pm.delete_group(ctgroup_id)
   return assert(u.prepare(db, QUERY_DELETE_GROUP, ctgroup_id))
end

local QUERY_RENAME_GROUP = [[
  UPDATE ctgroup SET name = ?
  WHERE ctgroup.id = ?
]]

function pm.rename_group(ctgroup_id, new_group_name)
   return assert(u.prepare(db, QUERY_RENAME_GROUP,
                           new_group_name, ctgroup_id))
end

--[[ PLAYER <--> GROUPS MAPPING ]]--

local QUERY_REGISTER_PLAYER_GROUP_PERMISSION = [[
  INSERT INTO player_ctgroup (player_id, ctgroup_id, permission)
  VALUES (?, ?, ?)
  ON CONFLICT DO NOTHING
]]

function pm.register_player_group_permission(player_id, ctgroup_id, permission)
   return assert(u.prepare(db, QUERY_REGISTER_PLAYER_GROUP_PERMISSION,
                         player_id, ctgroup_id, permission))
end

local QUERY_GET_PLAYER_GROUP_PERMISSION = [[
  SELECT * FROM player_ctgroup
  WHERE player_ctgroup.player_id = ?
    AND player_ctgroup.ctgroup_id = ?
]]

function pm.get_player_group(player_id, ctgroup_id)
   local cur = u.prepare(db, QUERY_GET_PLAYER_GROUP_PERMISSION,
                       player_id, ctgroup_id)
   if cur then
      return cur:fetch({}, "a")
   else
      return nil
   end
end

local QUERY_UPDATE_PLAYER_GROUP_PERMISSION = [[
  UPDATE player_ctgroup SET permission = ?
  WHERE player_ctgroup.player_id = ?
    AND player_ctgroup.ctgroup_id = ?
]]

function pm.update_player_group(player_id, ctgroup_id, permission)
   return assert(u.prepare(db, QUERY_UPDATE_PLAYER_GROUP_PERMISSION,
                         permission, player_id, ctgroup_id))
end

local QUERY_DELETE_PLAYER_GROUP = [[
  DELETE FROM player_ctgroup
  WHERE player_ctgroup.player_id = ?
    AND player_ctgroup.ctgroup_id = ?
]]

function pm.delete_player_group(player_id, ctgroup_id)
   return assert(u.prepare(db, QUERY_DELETE_PLAYER_GROUP,
                           player_id, ctgroup_id))
end

local QUERY_GET_PLAYERS_FOR_GROUP = [[
  SELECT player.id, player.name, player_ctgroup.permission
  FROM player
  INNER JOIN player_ctgroup
      ON player.id = player_ctgroup.player_id
     AND player_ctgroup.ctgroup_id = ?
]]

function pm.get_players_for_group(ctgroup_id)
   local cur = u.prepare(db, QUERY_GET_PLAYERS_FOR_GROUP, ctgroup_id)
   local players = {}
   local row = cur:fetch({}, "a")
   while row do
      -- TODO: clean up, table shallow copy helper func?
      table.insert(
         players,
         {
            name = row.name,
            id = row.id,
            permission = row.permission
         }
      )
      row = cur:fetch(row, "a")
   end
   return players
end

local QUERY_GET_GROUPS_FOR_PLAYER = [[
  SELECT ctgroup.id, ctgroup.name, player_ctgroup.permission
  FROM ctgroup
  INNER JOIN player_ctgroup
      ON ctgroup.id = player_ctgroup.ctgroup_id
     AND player_ctgroup.player_id = ?
]]

function pm.get_groups_for_player(player_id)
   local cur = u.prepare(db, QUERY_GET_GROUPS_FOR_PLAYER, player_id)
   local groups = {}
   local row = cur:fetch({}, "a")
   while row do
      -- TODO: clean up, table shallow copy helper func?
      table.insert(
         groups,
         {
            name = row.name,
            id = row.id,
            permission = row.permission
         }
      )
      row = cur:fetch(row, "a")
   end
   return groups
end

local QUERY_DELETE_PLAYERS_FOR_GROUP = [[
  DELETE FROM player_ctgroup
  WHERE player_ctgroup.ctgroup_id = ?
]]

function pm.delete_players_for_group(ctgroup_id)
   return assert(u.prepare(db, QUERY_DELETE_PLAYERS_FOR_GROUP, ctgroup_id))
end

--[[ End of DB interface ]]--

--[[ Command processing (eventually move framework to PMUtils) ]]--


function pm.send_chat_group(ctgroup_id, message)
    for _, playerinfo in ipairs(pm.get_players_for_group(ctgroup_id)) do
        minetest.chat_send_player(playerinfo.name, message)
    end
end


local function group_create_cmd(sender, group_name)
   if string.len(group_name) > 16 then
      return false, "Group name '"..group_name..
         "' is too long (16 character limit)."
   end

   if pm.get_group_by_name(group_name) then
      return false, "Group '"..group_name.."' already exists."
   end

   pm.register_group(group_name)
   local ctgroup = pm.get_group_by_name(group_name)
   pm.register_player_group_permission(sender.id, ctgroup.id, "admin")

   minetest.chat_send_player(
      sender.name,
      "Group '"..group_name.."' created successfully."
   )
   return true
end

local function titlecase_word(perm)
   local head = perm:sub(1, 1)
   local tail = perm:sub(2, perm:len())
   return head:upper() .. tail:lower()
end

local function group_info_cmd(sender, group_name)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   local permission = sender_group_info.permission
   local group_players_info = pm.get_players_for_group(ctgroup.id)

   local c = minetest.colorize

   minetest.chat_send_player(
      sender.name,
      c("#0a0", "[Group: ")..c("#fff", ctgroup.name)..c("#0a0", "]") .. "\n" ..
         c("#0a0", "[Id: ")..c("#fff", ctgroup.id)..c("#0a0", "]") .. "\n" ..
         "  Your permission level: "..permission
   )

   local info_table = {}
   for _, player_info in pairs(group_players_info) do
      local info_tab_entry = info_table[player_info.permission]
      if info_tab_entry then
         table.insert(info_table[player_info.permission], player_info.name)
      else
         info_table[player_info.permission] = { player_info.name }
      end
   end

   for perm, names in pairs(info_table) do
      minetest.chat_send_player(
         sender.name,
         "  " .. titlecase_word(perm) .. "s: " .. table.concat(names, ", ")
      )
   end

   return true
end

local function group_list_cmd(sender)
   local player_groups_info = pm.get_groups_for_player(sender.id)

   minetest.chat_send_player(sender.name, "Your groups:")

   local info_table = {}
   for _, group_info in pairs(player_groups_info) do
      local info_tab_entry = info_table[group_info.permission]
      if info_tab_entry then
         table.insert(info_table[group_info.permission], group_info.name)
      else
         info_table[group_info.permission] = { group_info.name }
      end
   end

   for perm, names in pairs(info_table) do
      minetest.chat_send_player(
         sender.name,
         "  " .. titlecase_word(perm) .. " of: " .. table.concat(names, ", ")
      )
   end

   return true
end

local function group_add_cmd(sender, group_name, ...)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   if sender_group_info.permission ~= "admin" then
      return false, "You don't have permission to do that."
   end

   local targets = { ... }
   for _, target in ipairs(targets) do
      local target_player = pm.get_player_by_name(target)
      if not target_player then
         minetest.chat_send_player(
            sender.name,
            "Player '"..target.."' not found."
         )
         goto continue
      end

      local target_player_group_info
         = pm.get_player_group(target_player.id, ctgroup.id)
      if target_player_group_info then
         minetest.chat_send_player(
            sender.name,
            "Player '"..target_player.name ..
               "' is already on the group '"..ctgroup.name.."'."
         )
         goto continue
      end

      pm.register_player_group_permission(target_player.id, ctgroup.id, "member")

      minetest.chat_send_player(
         sender.name,
         "Player '"..target_player.name.."' added to group '" ..
            ctgroup.name .. "'."
      )
      ::continue::
   end
   return true
end

local function group_remove_cmd(sender, group_name, ...)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   if sender_group_info.permission ~= "admin" then
      return false, "You don't have permission to do that."
   end

   local targets = { ... }
   for _, target in ipairs(targets) do
      local target_player = pm.get_player_by_name(target)
      if not target_player then
         minetest.chat_send_player(
            sender.name,
            "Player '"..target.."' not found."
         )
         goto continue
      end

      local target_player_group_info
         = pm.get_player_group(target_player.id, ctgroup.id)
      if not target_player_group_info then
         minetest.chat_send_player(
            sender.name,
            "Player '"..target_player.name ..
               "' is not on the group '"..ctgroup.name.."'."
         )
         goto continue
      end

      pm.delete_player_group(target_player.id, ctgroup.id)

      minetest.chat_send_player(
         sender.name,
         "Player '"..target_player.name.."' removed from group '" ..
            ctgroup.name .. "'."
      )
      ::continue::
   end
   return true
end

local function group_rank_cmd(sender, group_name, target, new_target_rank)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   if sender_group_info.permission ~= "admin" then
      return false, "You don't have permission to do that."
   end

   local target_player = pm.get_player_by_name(target)
   if not target_player then
      return false, "Player '"..target.."' not found."
   end

   local target_player_group_info
      = pm.get_player_group(target_player.id, ctgroup.id)
   if not target_player_group_info then
      return false, "Player '"..target_player.name ..
         "' is not on the group '"..ctgroup.name.."'."
   end
   if new_target_rank ~= "member" and
      new_target_rank ~= "mod" and
      new_target_rank ~= "admin"
   then
      return false, "Invalid permission '"..new_target_rank ..
         "', must be one of: member, mod, admin."
   end

   pm.update_player_group(target_player.id, ctgroup.id, new_target_rank)

   minetest.chat_send_player(
      sender.name,
      "Changed rank of player '"..target_player.name.."' to '" .. new_target_rank ..
         "' of group '"..ctgroup.name.."'."
   )
   return true
end

local function group_delete_cmd(sender, group_name, confirm)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   if sender_group_info.permission ~= "admin" then
      return false, "You don't have permission to do that."
   end

   if not confirm or
      confirm ~= "confirm"
   then
      return false, "You must confirm this action!"
   end

   pm.delete_players_for_group(ctgroup.id)
   pm.delete_group(ctgroup.id)

   minetest.chat_send_player(
      sender.name,
      "Deleted group '"..ctgroup.name.."'."
   )
   return true
end

local function group_rename_cmd(sender, group_name, new_group_name)
   local ctgroup = pm.get_group_by_name(group_name)
   if not ctgroup then
      return false, "Group '"..group_name.."' not found."
   end

   local sender_group_info = pm.get_player_group(sender.id, ctgroup.id)
   if not sender_group_info then
      return false, "You are not on the group '"..group_name.."'."
   end

   if sender_group_info.permission ~= "admin" then
      return false, "You don't have permission to do that."
   end

   if string.len(new_group_name) > 16 then
      return false, "Proposed name '"..new_group_name..
         "' is too long (16 character limit)."
   end

   pm.rename_group(ctgroup.id, new_group_name)

   minetest.chat_send_player(
      sender.name,
      "Renamed group '"..ctgroup.name.."' to '"..new_group_name.. "'."
   )
   return true
end

local cmd_lookup_table = {
   create = {
      params = { "<group>" },
      fn = group_create_cmd
   },
   delete = {
      params = { "<group>", "<confirm>" },
      fn = group_delete_cmd,
      accept_many_after = 1
   },
   info = {
      params = { "<group>" },
      fn = group_info_cmd
   },
   list = {
      params = {},
      fn = group_list_cmd
   },
   rename = {
      params = { "<group>", "<new_name>" },
      fn = group_rename_cmd
   },
   add = {
      params = { "<group>", "<players...>" },
      fn = group_add_cmd,
      accept_many_after = 2
   },
   remove = {
      params = { "<group>", "<players...>"},
      fn = group_remove_cmd,
      accept_many_after = 2
   },
   rank = {
      params = { "<group>", "<player>", "<new_rank>" },
      fn = group_rank_cmd
   }
}

local function pm_parse_params(pname, raw_params)
   local params = {}
   for chunk in string.gmatch(raw_params, "[^%s]+") do
      table.insert(params, chunk)
   end

   if #params == 0 then
      local actions = u.table_keyvals(cmd_lookup_table)
      return false, "Usage: /group <action> ...\n" ..
         "Valid actions: " .. table.concat(actions, ", ")
   end

   -- Pop the action from the parameters
   local action = table.remove(params, 1)
   local sender = pm.get_player_by_name(pname)

   local cmd_spec = cmd_lookup_table[action]
   if cmd_spec then
      local accept_many_after = cmd_spec.accept_many_after
      local accept_many = false

      if accept_many_after then
         accept_many = true
      else
         accept_many_after = 0
      end

      if #params ~= #cmd_spec.params or
         (accept_many and #params < accept_many_after)
      then
         return false, "Invalid arguments, usage: /group " .. action .. " "
            .. table.concat(cmd_spec.params, " ")
      end
      -- all cmd handler functions take the sender, and the parameters
      return cmd_spec.fn(sender, unpack(params))
   end

   return false, "Unknown action: '"..action.."'."
end


minetest.register_chatcommand("group", {
   params = "<action> <group name> [<params...>]",
   description = "PlayerManager group management.",
   func = function(pname, params)
      local sender = minetest.get_player_by_name(pname)
      if not sender then
         return false
      end
      local success, err = pm_parse_params(pname, params)
      if not success then
         minetest.chat_send_player(pname, "Error: "..err)
         return false
      end
      return true
   end
})


minetest.register_on_joinplayer(function(player)
      local pname = player:get_player_name(player)
      if not pm.get_player_by_name(pname) then
         pm.register_player(pname)
      end
end)

return pm
