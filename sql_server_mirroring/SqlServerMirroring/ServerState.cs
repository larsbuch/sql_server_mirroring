﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class ServerState
    {
        private ServerStateEnum _state;
        private bool _isDegradedState;
        private bool _ignoreMirrorStateCheck;
        private List<ServerStateEnum> _validNewStates;

        public ServerState(ServerStateEnum state, bool isDegradedState, bool ignoreMirrorStateCheck, List<ServerStateEnum> validNewStates)
        {
            _state = state;
            _isDegradedState = isDegradedState;
            _ignoreMirrorStateCheck = ignoreMirrorStateCheck;
            _validNewStates = validNewStates;
        }

        #region Properties

        public override string ToString()
        {
            return _state.ToString();
        }

        public ServerStateEnum State
        {
            get
            {
                return _state;
            }
        }

        public bool IsDegradedState
        {
            get
            {
                return _isDegradedState;
            }
        }

        public bool IgnoreMirrorStateCheck
        {
            get
            {
                return _ignoreMirrorStateCheck;
            }
        }

        public List<ServerStateEnum> ValidNewStates
        {
            get
            {
                return _validNewStates;
            }
        }

        public bool ValidTransition(ServerStateEnum newState)
        {
            if (ValidNewStates.Contains(newState))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion
    }

    public enum ServerStateEnum
    {
        INITIAL_STATE,
        PRIMARY_STARTUP_STATE,
        PRIMARY_RUNNING_STATE,
        PRIMARY_FORCED_RUNNING_STATE,
        PRIMARY_SHUTTING_DOWN_STATE,
        PRIMARY_SHUTDOWN_STATE,
        PRIMARY_MAINTENANCE_STATE,
        PRIMARY_MANUAL_FAILOVER_STATE,
        PRIMARY_RUNNING_NO_SECONDARY_STATE,
        SECONDARY_STARTUP_STATE,
        SECONDARY_RUNNING_STATE,
        SECONDARY_SHUTTING_DOWN_STATE,
        SECONDARY_SHUTDOWN_STATE,
        SECONDARY_MAINTENANCE_STATE,
        SECONDARY_MANUAL_FAILOVER_STATE,
        SECONDARY_FORCED_MANUAL_FAILOVER_STATE,
        SECONDARY_RUNNING_NO_PRIMARY_STATE
    }
}
