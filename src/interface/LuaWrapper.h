#ifndef LUA_WRAP_H__
#define LUA_WRAP_H__

#include <lua.hpp>

class Lua {
public:
	Lua(bool loadlibs = true) { _state = luaL_newstate(); if(loadlibs) luaL_openlibs(_state); }
	~Lua() { lua_close(_state); }
	operator lua_State* () { return _state; }
private:
	lua_State *_state;
};

#endif

