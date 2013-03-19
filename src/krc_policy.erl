%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Conflict resolution (GET) policies.
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(krc_policy).

%%%_* Exports ==========================================================
-export([ behaviour_info/1
        ]).

%%%_* Code =============================================================
%%-callback lookup(_, _) -> [krc_resolver:resolver() |
%%                           krc_resolver:resolution_procedure()].
behaviour_info(callbacks) ->
  [ {lookup, 2}
  ];
behaviour_info(_) -> undefined.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
