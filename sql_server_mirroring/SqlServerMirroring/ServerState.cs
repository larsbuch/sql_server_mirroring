using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MirrorLib
{
    public class ServerState
    {
        private ServerStateEnum _state;
        private MirrorState _mirrorState;
        private bool _isPrimaryRole;
        private List<ServerStateEnum> _validNewStates;
        private int _serverStateCount;
        private CountStates _countStates;

        public ServerState(ServerStateEnum state, CountStates countStates, MirrorState mirrorState, List<ServerStateEnum> validNewStates)
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
            _mirrorState = mirrorState;
            _validNewStates = validNewStates;
            _countStates = countStates;
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

        public MirrorState MirrorState
        {
            get
            {
                return _mirrorState;
            }
        }

        public List<ServerStateEnum> ValidNewStates
        {
            get
            {
                return _validNewStates;
            }
        }

        public CountStates CountStates
        {
            get
            {
                return _countStates;
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
                if (_countStates == CountStates.Yes)
                {
                    _serverStateCount = value;
                }
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

        public bool MasterDatabaseTablesSafeToAccess
        {
            get
            {
                return State != ServerStateEnum.NOT_SET &&
                        State != ServerStateEnum.PRIMARY_INITIAL_STATE &&
                        State != ServerStateEnum.PRIMARY_CONFIGURATION_STATE &&
                        State != ServerStateEnum.SECONDARY_INITIAL_STATE &&
                        State != ServerStateEnum.SECONDARY_CONFIGURATION_STATE;
            }
        }

        #endregion
    }
}
