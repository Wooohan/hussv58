import React, { useState, useEffect } from 'react';
import { Users, Activity, DollarSign, Server, Edit2, Save, X, Search, Ban, UserPlus, Shield, Trash2, CheckCircle, RefreshCw } from 'lucide-react';
import { User, BlockedIP } from '../types';
import { 
  fetchUsersFromSupabase, 
  createUserInSupabase, 
  updateUserInSupabase, 
  deleteUserFromSupabase,
  fetchBlockedIPsFromSupabase,
  blockIPInSupabase,
  unblockIPInSupabase
} from '../services/userService';

export const AdminPanel: React.FC = () => {
  const [users, setUsers] = useState<User[]>([]);
  const [blockedIPs, setBlockedIPs] = useState<BlockedIP[]>([]);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<Partial<User>>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState<'users' | 'blocked' | 'add'>('users');
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null);
  
  // Add user form state
  const [newUserName, setNewUserName] = useState('');
  const [newUserEmail, setNewUserEmail] = useState('');
  const [newUserPlan, setNewUserPlan] = useState<'Free' | 'Starter' | 'Pro' | 'Enterprise'>('Free');
  const [newUserRole, setNewUserRole] = useState<'user' | 'admin'>('user');
  
  // Block IP form state
  const [blockIpAddress, setBlockIpAddress] = useState('');
  const [blockReason, setBlockReason] = useState('');

  // Load data from Supabase on mount
  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setIsLoading(true);
    try {
      const [usersData, blockedIPsData] = await Promise.all([
        fetchUsersFromSupabase(),
        fetchBlockedIPsFromSupabase()
      ]);
      setUsers(usersData);
      setBlockedIPs(blockedIPsData);
    } catch (err) {
      console.error('Error loading data:', err);
      showMessage('error', 'Failed to load data from database');
    }
    setIsLoading(false);
  };

  const showMessage = (type: 'success' | 'error', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  const activeUsers = users.filter(u => u.isOnline).length;
  const totalRevenue = users.reduce((acc, user) => {
    if (user.plan === 'Pro') return acc + 149;
    if (user.plan === 'Enterprise') return acc + 499;
    return acc;
  }, 0);

  const handleEdit = (user: User) => {
    setEditingId(user.id);
    setEditForm(user);
  };

  const handleSave = async () => {
    if (!editingId) return;
    
    setIsSaving(true);
    const updatedUser = { ...users.find(u => u.id === editingId)!, ...editForm };
    
    const success = await updateUserInSupabase(updatedUser);
    
    if (success) {
      setUsers(users.map(u => (u.id === editingId ? updatedUser : u)));
      showMessage('success', 'User updated successfully');
    } else {
      showMessage('error', 'Failed to update user');
    }
    
    setEditingId(null);
    setIsSaving(false);
  };

  const handleBlockUser = async (user: User) => {
    setIsSaving(true);
    const updatedUser = { ...user, isBlocked: !user.isBlocked };
    
    const success = await updateUserInSupabase(updatedUser);
    
    if (success) {
      setUsers(users.map(u => u.id === user.id ? updatedUser : u));
      showMessage('success', user.isBlocked ? 'User unblocked' : 'User blocked');
    } else {
      showMessage('error', 'Failed to update user status');
    }
    
    setIsSaving(false);
  };

  const handleBlockIP = async () => {
    if (!blockIpAddress.trim()) return;
    
    setIsSaving(true);
    const success = await blockIPInSupabase(blockIpAddress.trim(), blockReason.trim());
    
    if (success) {
      const newBlockedIP: BlockedIP = {
        ip: blockIpAddress.trim(),
        blockedAt: new Date().toISOString(),
        reason: blockReason.trim() || 'No reason provided'
      };
      setBlockedIPs([newBlockedIP, ...blockedIPs]);
      setBlockIpAddress('');
      setBlockReason('');
      showMessage('success', 'IP address blocked');
    } else {
      showMessage('error', 'Failed to block IP address');
    }
    
    setIsSaving(false);
  };

  const handleUnblockIP = async (ip: string) => {
    setIsSaving(true);
    const success = await unblockIPInSupabase(ip);
    
    if (success) {
      setBlockedIPs(blockedIPs.filter(b => b.ip !== ip));
      showMessage('success', 'IP address unblocked');
    } else {
      showMessage('error', 'Failed to unblock IP address');
    }
    
    setIsSaving(false);
  };

  const handleAddUser = async () => {
    if (!newUserName.trim() || !newUserEmail.trim()) {
      showMessage('error', 'Name and email are required');
      return;
    }
    
    // Check if email already exists
    if (users.find(u => u.email.toLowerCase() === newUserEmail.toLowerCase())) {
      showMessage('error', 'User with this email already exists!');
      return;
    }

    setIsSaving(true);
    
    const randomIp = `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`;
    
    const newUser: User = {
      id: `user-${Date.now()}`,
      name: newUserName.trim(),
      email: newUserEmail.trim().toLowerCase(),
      role: newUserRole,
      plan: newUserPlan,
      dailyLimit: newUserPlan === 'Free' ? 50 : newUserPlan === 'Starter' ? 100 : newUserPlan === 'Pro' ? 500 : 100000,
      recordsExtractedToday: 0,
      lastActive: 'Never',
      ipAddress: randomIp,
      isOnline: false,
      isBlocked: false
    };

    const createdUser = await createUserInSupabase(newUser);
    
    if (createdUser) {
      setUsers([createdUser, ...users]);
      // Reset form
      setNewUserName('');
      setNewUserEmail('');
      setNewUserPlan('Free');
      setNewUserRole('user');
      setActiveTab('users');
      showMessage('success', 'User created successfully');
    } else {
      showMessage('error', 'Failed to create user');
    }
    
    setIsSaving(false);
  };

  const handleDeleteUser = async (userId: string) => {
    // Prevent deleting admin
    const userToDelete = users.find(u => u.id === userId);
    if (userToDelete?.role === 'admin') {
      showMessage('error', 'Cannot delete admin user!');
      return;
    }
    
    if (!confirm('Are you sure you want to delete this user?')) return;
    
    setIsSaving(true);
    const success = await deleteUserFromSupabase(userId);
    
    if (success) {
      setUsers(users.filter(u => u.id !== userId));
      showMessage('success', 'User deleted successfully');
    } else {
      showMessage('error', 'Failed to delete user');
    }
    
    setIsSaving(false);
  };

  const filteredUsers = users.filter(u => 
    u.name.toLowerCase().includes(searchTerm.toLowerCase()) || 
    u.email.toLowerCase().includes(searchTerm.toLowerCase())
  );

  if (isLoading) {
    return (
      <div className="p-8 flex items-center justify-center h-full">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mx-auto mb-4"></div>
          <p className="text-slate-400">Loading admin data...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 space-y-8 animate-fade-in h-full overflow-y-auto">
      {/* Message Toast */}
      {message && (
        <div className={`fixed top-4 right-4 z-50 px-6 py-3 rounded-lg shadow-lg ${
          message.type === 'success' ? 'bg-green-600 text-white' : 'bg-red-600 text-white'
        }`}>
          {message.text}
        </div>
      )}

      <div className="flex justify-between items-end">
        <div>
          <h1 className="text-3xl font-bold text-white mb-2">Admin Control Center</h1>
          <p className="text-slate-400">System analytics, user management, and IP blocking.</p>
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={loadData}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg transition-colors"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <div className="flex items-center gap-2 bg-red-500/10 border border-red-500/20 px-4 py-2 rounded-full">
             <div className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
             <span className="text-red-400 font-mono text-xs">LIVE ENVIRONMENT</span>
          </div>
        </div>
      </div>

      {/* Admin Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {[
          { label: 'Active Sessions', value: activeUsers, icon: Activity, color: 'text-green-400' },
          { label: 'Total Users', value: users.length, icon: Users, color: 'text-blue-400' },
          { label: 'Monthly Revenue', value: `$${totalRevenue}`, icon: DollarSign, color: 'text-yellow-400' },
          { label: 'Blocked IPs', value: blockedIPs.length, icon: Ban, color: 'text-red-400' },
        ].map((stat, idx) => (
          <div key={idx} className="bg-slate-800/50 border border-slate-700 p-6 rounded-2xl">
            <div className="flex justify-between items-start mb-2">
              <span className="text-slate-400 text-sm font-medium">{stat.label}</span>
              <stat.icon className={`w-5 h-5 ${stat.color}`} />
            </div>
            <p className="text-3xl font-bold text-white">{stat.value}</p>
          </div>
        ))}
      </div>

      {/* Tab Navigation */}
      <div className="flex gap-2 border-b border-slate-700 pb-4">
        <button
          onClick={() => setActiveTab('users')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            activeTab === 'users' 
              ? 'bg-indigo-600 text-white' 
              : 'text-slate-400 hover:text-white hover:bg-slate-800'
          }`}
        >
          <Users className="w-4 h-4 inline mr-2" />
          User Management
        </button>
        <button
          onClick={() => setActiveTab('blocked')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            activeTab === 'blocked' 
              ? 'bg-indigo-600 text-white' 
              : 'text-slate-400 hover:text-white hover:bg-slate-800'
          }`}
        >
          <Ban className="w-4 h-4 inline mr-2" />
          Blocked IPs ({blockedIPs.length})
        </button>
        <button
          onClick={() => setActiveTab('add')}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            activeTab === 'add' 
              ? 'bg-indigo-600 text-white' 
              : 'text-slate-400 hover:text-white hover:bg-slate-800'
          }`}
        >
          <UserPlus className="w-4 h-4 inline mr-2" />
          Add User
        </button>
      </div>

      {/* Users Tab */}
      {activeTab === 'users' && (
        <div className="bg-slate-800/50 border border-slate-700 rounded-2xl overflow-hidden flex flex-col min-h-[500px]">
          <div className="p-6 border-b border-slate-700 flex justify-between items-center">
            <h2 className="text-lg font-bold text-white">User Directory ({users.length} users)</h2>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 w-4 h-4" />
              <input 
                type="text" 
                placeholder="Search users..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="bg-slate-900 border border-slate-700 rounded-lg pl-10 pr-4 py-2 text-sm text-white outline-none focus:border-indigo-500"
              />
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm text-slate-400">
              <thead className="bg-slate-900 text-slate-200">
                <tr>
                  <th className="p-4 font-medium">Status</th>
                  <th className="p-4 font-medium">Identity</th>
                  <th className="p-4 font-medium">Role</th>
                  <th className="p-4 font-medium">Plan</th>
                  <th className="p-4 font-medium">Daily Limit</th>
                  <th className="p-4 font-medium">Usage</th>
                  <th className="p-4 font-medium">IP Address</th>
                  <th className="p-4 font-medium">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {filteredUsers.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="p-8 text-center text-slate-400">
                      {searchTerm ? 'No users found matching your search.' : 'No users in database. Add a user to get started.'}
                    </td>
                  </tr>
                ) : (
                  filteredUsers.map((user) => (
                    <tr key={user.id} className={`hover:bg-slate-700/30 transition-colors ${user.isBlocked ? 'opacity-50 bg-red-900/10' : ''}`}>
                      <td className="p-4">
                        <div className="flex items-center gap-2">
                          <div className={`w-2 h-2 rounded-full ${user.isBlocked ? 'bg-red-500' : user.isOnline ? 'bg-green-500' : 'bg-slate-500'}`} />
                          <span>{user.isBlocked ? 'Blocked' : user.isOnline ? 'Online' : user.lastActive}</span>
                        </div>
                      </td>
                      <td className="p-4">
                        <div className="font-bold text-white">{user.name}</div>
                        <div className="text-xs">{user.email}</div>
                      </td>
                      <td className="p-4">
                        <span className={`px-2 py-1 rounded text-xs font-bold ${
                          user.role === 'admin' ? 'bg-red-500/20 text-red-300' : 'bg-slate-600/20 text-slate-300'
                        }`}>
                          {user.role.toUpperCase()}
                        </span>
                      </td>
                      <td className="p-4">
                         {editingId === user.id ? (
                            <select 
                              className="bg-slate-900 border border-slate-600 rounded p-1 text-white"
                              value={editForm.plan}
                              onChange={e => setEditForm({...editForm, plan: e.target.value as any})}
                            >
                              <option value="Free">Free</option>
                              <option value="Starter">Starter</option>
                              <option value="Pro">Pro</option>
                              <option value="Enterprise">Enterprise</option>
                            </select>
                         ) : (
                            <span className={`px-2 py-1 rounded text-xs font-bold 
                              ${user.plan === 'Enterprise' ? 'bg-purple-500/20 text-purple-300' : 
                                user.plan === 'Pro' ? 'bg-indigo-500/20 text-indigo-300' : 'bg-slate-600/20 text-slate-300'}`}>
                              {user.plan}
                            </span>
                         )}
                      </td>
                      <td className="p-4 font-mono">
                        {editingId === user.id ? (
                          <input 
                            type="number"
                            className="bg-slate-900 border border-slate-600 rounded p-1 text-white w-24"
                            value={editForm.dailyLimit}
                            onChange={e => setEditForm({...editForm, dailyLimit: parseInt(e.target.value)})}
                          />
                        ) : (
                          user.dailyLimit.toLocaleString()
                        )}
                      </td>
                      <td className="p-4">
                        <div className="flex items-center gap-2">
                          <div className="w-24 bg-slate-700 rounded-full h-1.5">
                            <div 
                              className={`h-1.5 rounded-full ${user.recordsExtractedToday > user.dailyLimit * 0.9 ? 'bg-red-500' : 'bg-indigo-500'}`} 
                              style={{ width: `${Math.min((user.recordsExtractedToday / user.dailyLimit) * 100, 100)}%` }}
                            />
                          </div>
                          <span className="text-xs">{user.recordsExtractedToday}</span>
                        </div>
                      </td>
                      <td className="p-4 font-mono text-xs text-slate-500">{user.ipAddress}</td>
                      <td className="p-4">
                        <div className="flex gap-2">
                          {editingId === user.id ? (
                            <>
                              <button 
                                onClick={handleSave} 
                                disabled={isSaving}
                                className="p-1.5 bg-green-500/20 text-green-400 rounded hover:bg-green-500/30 disabled:opacity-50" 
                                title="Save"
                              >
                                <Save size={16} />
                              </button>
                              <button onClick={() => setEditingId(null)} className="p-1.5 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30" title="Cancel">
                                <X size={16} />
                              </button>
                            </>
                          ) : (
                            <>
                              <button onClick={() => handleEdit(user)} className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-700 rounded transition-colors" title="Edit">
                                <Edit2 size={16} />
                              </button>
                              <button 
                                onClick={() => handleBlockUser(user)} 
                                disabled={isSaving}
                                className={`p-1.5 rounded transition-colors disabled:opacity-50 ${user.isBlocked ? 'bg-green-500/20 text-green-400 hover:bg-green-500/30' : 'bg-red-500/20 text-red-400 hover:bg-red-500/30'}`}
                                title={user.isBlocked ? 'Unblock User' : 'Block User'}
                              >
                                {user.isBlocked ? <CheckCircle size={16} /> : <Ban size={16} />}
                              </button>
                              {user.role !== 'admin' && (
                                <button 
                                  onClick={() => handleDeleteUser(user.id)} 
                                  disabled={isSaving}
                                  className="p-1.5 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-colors disabled:opacity-50"
                                  title="Delete User"
                                >
                                  <Trash2 size={16} />
                                </button>
                              )}
                            </>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Blocked IPs Tab */}
      {activeTab === 'blocked' && (
        <div className="space-y-6">
          {/* Block IP Form */}
          <div className="bg-slate-800/50 border border-slate-700 rounded-2xl p-6">
            <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
              <Ban className="w-5 h-5 text-red-400" />
              Block New IP Address
            </h3>
            <div className="flex gap-4">
              <div className="flex-1">
                <label className="block text-sm text-slate-400 mb-1">IP Address</label>
                <input
                  type="text"
                  placeholder="e.g., 192.168.1.100"
                  value={blockIpAddress}
                  onChange={(e) => setBlockIpAddress(e.target.value)}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-2 text-white outline-none focus:border-indigo-500"
                />
              </div>
              <div className="flex-1">
                <label className="block text-sm text-slate-400 mb-1">Reason (optional)</label>
                <input
                  type="text"
                  placeholder="e.g., Suspicious activity"
                  value={blockReason}
                  onChange={(e) => setBlockReason(e.target.value)}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-2 text-white outline-none focus:border-indigo-500"
                />
              </div>
              <div className="flex items-end">
                <button
                  onClick={handleBlockIP}
                  disabled={isSaving || !blockIpAddress.trim()}
                  className="bg-red-600 hover:bg-red-500 text-white px-6 py-2 rounded-lg font-medium transition-colors disabled:opacity-50"
                >
                  Block IP
                </button>
              </div>
            </div>
          </div>

          {/* Blocked IPs List */}
          <div className="bg-slate-800/50 border border-slate-700 rounded-2xl overflow-hidden">
            <div className="p-6 border-b border-slate-700">
              <h3 className="text-lg font-bold text-white">Blocked IP Addresses</h3>
            </div>
            {blockedIPs.length === 0 ? (
              <div className="p-12 text-center text-slate-400">
                <Shield className="w-12 h-12 mx-auto mb-4 opacity-50" />
                <p>No IP addresses are currently blocked.</p>
              </div>
            ) : (
              <table className="w-full text-left text-sm text-slate-400">
                <thead className="bg-slate-900 text-slate-200">
                  <tr>
                    <th className="p-4 font-medium">IP Address</th>
                    <th className="p-4 font-medium">Blocked At</th>
                    <th className="p-4 font-medium">Reason</th>
                    <th className="p-4 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {blockedIPs.map((blocked, idx) => (
                    <tr key={idx} className="hover:bg-slate-700/30 transition-colors">
                      <td className="p-4 font-mono text-white">{blocked.ip}</td>
                      <td className="p-4">{new Date(blocked.blockedAt).toLocaleString()}</td>
                      <td className="p-4">{blocked.reason}</td>
                      <td className="p-4">
                        <button
                          onClick={() => handleUnblockIP(blocked.ip)}
                          disabled={isSaving}
                          className="px-3 py-1 bg-green-500/20 text-green-400 rounded hover:bg-green-500/30 transition-colors text-xs font-medium disabled:opacity-50"
                        >
                          Unblock
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}

      {/* Add User Tab */}
      {activeTab === 'add' && (
        <div className="bg-slate-800/50 border border-slate-700 rounded-2xl p-6 max-w-2xl">
          <h3 className="text-lg font-bold text-white mb-6 flex items-center gap-2">
            <UserPlus className="w-5 h-5 text-indigo-400" />
            Add New User
          </h3>
          <div className="space-y-4">
            <div>
              <label className="block text-sm text-slate-400 mb-1">Full Name *</label>
              <input
                type="text"
                placeholder="John Doe"
                value={newUserName}
                onChange={(e) => setNewUserName(e.target.value)}
                className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white outline-none focus:border-indigo-500"
              />
            </div>
            <div>
              <label className="block text-sm text-slate-400 mb-1">Email Address *</label>
              <input
                type="email"
                placeholder="john@company.com"
                value={newUserEmail}
                onChange={(e) => setNewUserEmail(e.target.value)}
                className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white outline-none focus:border-indigo-500"
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm text-slate-400 mb-1">Plan</label>
                <select
                  value={newUserPlan}
                  onChange={(e) => setNewUserPlan(e.target.value as any)}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white outline-none focus:border-indigo-500"
                >
                  <option value="Free">Free (50 MCs/day)</option>
                  <option value="Starter">Starter (100 MCs/day)</option>
                  <option value="Pro">Pro (500 MCs/day)</option>
                  <option value="Enterprise">Enterprise (Unlimited)</option>
                </select>
              </div>
              <div>
                <label className="block text-sm text-slate-400 mb-1">Role</label>
                <select
                  value={newUserRole}
                  onChange={(e) => setNewUserRole(e.target.value as any)}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white outline-none focus:border-indigo-500"
                >
                  <option value="user">User</option>
                  <option value="admin">Admin</option>
                </select>
              </div>
            </div>
            <div className="pt-4">
              <button
                onClick={handleAddUser}
                disabled={isSaving || !newUserName.trim() || !newUserEmail.trim()}
                className="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-3 rounded-xl transition-all disabled:opacity-50"
              >
                {isSaving ? 'Creating...' : 'Create User Account'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
