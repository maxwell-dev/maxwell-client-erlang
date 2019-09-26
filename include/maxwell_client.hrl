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

-define(ON_CONNECTED_CMD(Ref, Pid), {'$on_connected', Ref, Pid}).
-define(ON_DISCONNECTED_CMD(Ref, Pid), {'$on_disconnected', Ref, Pid}).

-endif.
