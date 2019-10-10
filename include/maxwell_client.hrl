%%%-------------------------------------------------------------------
%%% @author xuchaoqian
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Sep 2018 2:08 PM
%%%-------------------------------------------------------------------

-ifndef(maxwell_client).
-define(maxwell_client, true).

-define(ON_CONNECTED_CMD(Pid), {'$on_connected', Pid}).
-define(ON_DISCONNECTED_CMD(Pid), {'$on_disconnected', Pid}).

-endif.
