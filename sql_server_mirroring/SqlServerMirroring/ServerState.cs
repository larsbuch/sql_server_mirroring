using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlServerMirroring
{
    public class ServerState
    {
        private ServerStateEnum _state;
        private bool _isDegradedState;
        private List<ServerStateEnum> _validNewStates;

        public ServerState(ServerStateEnum state, bool isDegradedState, List<ServerStateEnum> validNewStates)
        {
            _state = state;
            _isDegradedState = isDegradedState;
            _validNewStates = validNewStates;
        }

        #region Properties

        public string StateName
        {
            get
            {
                switch (_state)
                {
                    case ServerStateEnum.STARTUP_STATE:
                        return "Startup State";
                    case ServerStateEnum.RUNNING_STATE:
                        return "Running State";
                    case ServerStateEnum.FORCED_RUNNING_STATE:
                        return "Forced Running State";
                    case ServerStateEnum.SHUTTING_DOWN_STATE:
                        return "Shutting Down State";
                    case ServerStateEnum.SHUTDOWN_STATE:
                        return "Shutdown State";
                    case ServerStateEnum.MAINTENANCE_STATE:
                        return "Maintenance State";
                    case ServerStateEnum.MANUAL_FAILOVER_STATE:
                        return "Manual Failover State";
                    case ServerStateEnum.FORCED_MANUAL_FAILOVER_STATE:
                        return "Forced Manual Failover State";
                    default:
                        return "Unknown State";
                }
            }
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
        STARTUP_STATE,
        RUNNING_STATE,
        FORCED_RUNNING_STATE,
        SHUTTING_DOWN_STATE,
        SHUTDOWN_STATE,
        MAINTENANCE_STATE,
        MANUAL_FAILOVER_STATE,
        FORCED_MANUAL_FAILOVER_STATE

    }
}
