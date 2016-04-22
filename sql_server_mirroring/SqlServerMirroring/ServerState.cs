using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class ServerState
    {
        private ServerStateEnum _state;
        private bool _isDegradedState;
        private bool _isPrimaryRole;
        private List<ServerStateEnum> _validNewStates;
        private int _serverStateCount;

        public ServerState(ServerStateEnum state, bool isDegradedState, List<ServerStateEnum> validNewStates)
        {
            _state = state;
            if(state.ToString().StartsWith("PRIMARY"))
            {
                _isPrimaryRole = true;
            }
            else
            {
                _isPrimaryRole = false;
            }
            _isDegradedState = isDegradedState;
            _validNewStates = validNewStates;
            _serverStateCount = 0;
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

        public bool IsPrimaryRole
        {
            get
            {
                return _isPrimaryRole;
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

        public int ServerStateCount
        {
            get
            {
                return _serverStateCount;
            }
            set
            {
                _serverStateCount = value;
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
}
