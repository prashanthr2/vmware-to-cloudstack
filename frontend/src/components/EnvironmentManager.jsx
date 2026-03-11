import { useMemo, useState } from "react";

function makeId(prefix) {
  return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
}

const defaultVcenterForm = {
  name: "",
  host: "",
  username: "",
  password: "",
};

const defaultCloudstackForm = {
  name: "",
  apiUrl: "",
  apiKey: "",
  secretKey: "",
};

export default function EnvironmentManager({ envState, onChange, onToast }) {
  const [vcForm, setVcForm] = useState(defaultVcenterForm);
  const [csForm, setCsForm] = useState(defaultCloudstackForm);
  const [vcEditId, setVcEditId] = useState("");
  const [csEditId, setCsEditId] = useState("");

  const selectedVcenter = useMemo(
    () => envState.vcenters.find((item) => item.id === envState.selectedVcenterId),
    [envState]
  );
  const selectedCloudstack = useMemo(
    () => envState.cloudstacks.find((item) => item.id === envState.selectedCloudstackId),
    [envState]
  );

  const setSelected = (type, id) => {
    onChange({
      ...envState,
      [type]: id,
    });
  };

  const saveVcenter = () => {
    if (!vcForm.name || !vcForm.host || !vcForm.username || !vcForm.password) {
      onToast("error", "vCenter env requires name, host, username, and password.");
      return;
    }

    if (vcEditId) {
      onChange({
        ...envState,
        vcenters: envState.vcenters.map((item) => (item.id === vcEditId ? { ...item, ...vcForm } : item)),
      });
      onToast("success", "Updated vCenter environment.");
    } else {
      const newItem = { id: makeId("vc"), ...vcForm };
      onChange({
        ...envState,
        vcenters: [...envState.vcenters, newItem],
        selectedVcenterId: envState.selectedVcenterId || newItem.id,
      });
      onToast("success", "Added vCenter environment.");
    }

    setVcForm(defaultVcenterForm);
    setVcEditId("");
  };

  const saveCloudstack = () => {
    if (!csForm.name || !csForm.apiUrl || !csForm.apiKey || !csForm.secretKey) {
      onToast("error", "CloudStack env requires name, API URL, API key, and secret key.");
      return;
    }

    if (csEditId) {
      onChange({
        ...envState,
        cloudstacks: envState.cloudstacks.map((item) => (item.id === csEditId ? { ...item, ...csForm } : item)),
      });
      onToast("success", "Updated CloudStack environment.");
    } else {
      const newItem = { id: makeId("cs"), ...csForm };
      onChange({
        ...envState,
        cloudstacks: [...envState.cloudstacks, newItem],
        selectedCloudstackId: envState.selectedCloudstackId || newItem.id,
      });
      onToast("success", "Added CloudStack environment.");
    }

    setCsForm(defaultCloudstackForm);
    setCsEditId("");
  };

  const editVcenter = (item) => {
    setVcEditId(item.id);
    setVcForm({
      name: item.name || "",
      host: item.host || "",
      username: item.username || "",
      password: item.password || "",
    });
  };

  const editCloudstack = (item) => {
    setCsEditId(item.id);
    setCsForm({
      name: item.name || "",
      apiUrl: item.apiUrl || "",
      apiKey: item.apiKey || "",
      secretKey: item.secretKey || "",
    });
  };

  const removeItem = (kind, id) => {
    if (kind === "vcenter") {
      const remaining = envState.vcenters.filter((item) => item.id !== id);
      onChange({
        ...envState,
        vcenters: remaining,
        selectedVcenterId: envState.selectedVcenterId === id ? remaining[0]?.id || "" : envState.selectedVcenterId,
      });
      if (vcEditId === id) {
        setVcEditId("");
        setVcForm(defaultVcenterForm);
      }
    } else {
      const remaining = envState.cloudstacks.filter((item) => item.id !== id);
      onChange({
        ...envState,
        cloudstacks: remaining,
        selectedCloudstackId:
          envState.selectedCloudstackId === id ? remaining[0]?.id || "" : envState.selectedCloudstackId,
      });
      if (csEditId === id) {
        setCsEditId("");
        setCsForm(defaultCloudstackForm);
      }
    }
  };

  return (
    <section className="panel">
      <h2>Environment Manager</h2>
      <p className="hint">Profiles are stored locally in this browser.</p>

      <div className="env-grid">
        <div className="env-card">
          <h3>vCenter</h3>
          <label>
            Active vCenter
            <select
              value={envState.selectedVcenterId}
              onChange={(e) => setSelected("selectedVcenterId", e.target.value)}
            >
              <option value="">Select vCenter</option>
              {envState.vcenters.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          {selectedVcenter ? <p className="hint">Using {selectedVcenter.host}</p> : null}

          <div className="stacked-fields">
            <input
              placeholder="Name"
              value={vcForm.name}
              onChange={(e) => setVcForm((prev) => ({ ...prev, name: e.target.value }))}
            />
            <input
              placeholder="Host / URL"
              value={vcForm.host}
              onChange={(e) => setVcForm((prev) => ({ ...prev, host: e.target.value }))}
            />
            <input
              placeholder="Username"
              value={vcForm.username}
              onChange={(e) => setVcForm((prev) => ({ ...prev, username: e.target.value }))}
            />
            <input
              placeholder="Password"
              type="password"
              value={vcForm.password}
              onChange={(e) => setVcForm((prev) => ({ ...prev, password: e.target.value }))}
            />
          </div>
          <div className="actions compact">
            <button onClick={saveVcenter}>{vcEditId ? "Update" : "Add"}</button>
            {vcEditId ? (
              <button
                className="secondary"
                onClick={() => {
                  setVcEditId("");
                  setVcForm(defaultVcenterForm);
                }}
              >
                Cancel
              </button>
            ) : null}
          </div>

          <div className="env-list">
            {envState.vcenters.map((item) => (
              <div key={item.id} className="env-list-row">
                <span>{item.name}</span>
                <div className="row-actions">
                  <button className="secondary" onClick={() => editVcenter(item)}>
                    Edit
                  </button>
                  <button className="danger" onClick={() => removeItem("vcenter", item.id)}>
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="env-card">
          <h3>CloudStack</h3>
          <label>
            Active CloudStack
            <select
              value={envState.selectedCloudstackId}
              onChange={(e) => setSelected("selectedCloudstackId", e.target.value)}
            >
              <option value="">Select CloudStack</option>
              {envState.cloudstacks.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          {selectedCloudstack ? <p className="hint">Using {selectedCloudstack.apiUrl}</p> : null}

          <div className="stacked-fields">
            <input
              placeholder="Name"
              value={csForm.name}
              onChange={(e) => setCsForm((prev) => ({ ...prev, name: e.target.value }))}
            />
            <input
              placeholder="API URL"
              value={csForm.apiUrl}
              onChange={(e) => setCsForm((prev) => ({ ...prev, apiUrl: e.target.value }))}
            />
            <input
              placeholder="API Key"
              value={csForm.apiKey}
              onChange={(e) => setCsForm((prev) => ({ ...prev, apiKey: e.target.value }))}
            />
            <input
              placeholder="Secret Key"
              type="password"
              value={csForm.secretKey}
              onChange={(e) => setCsForm((prev) => ({ ...prev, secretKey: e.target.value }))}
            />
          </div>
          <div className="actions compact">
            <button onClick={saveCloudstack}>{csEditId ? "Update" : "Add"}</button>
            {csEditId ? (
              <button
                className="secondary"
                onClick={() => {
                  setCsEditId("");
                  setCsForm(defaultCloudstackForm);
                }}
              >
                Cancel
              </button>
            ) : null}
          </div>

          <div className="env-list">
            {envState.cloudstacks.map((item) => (
              <div key={item.id} className="env-list-row">
                <span>{item.name}</span>
                <div className="row-actions">
                  <button className="secondary" onClick={() => editCloudstack(item)}>
                    Edit
                  </button>
                  <button className="danger" onClick={() => removeItem("cloudstack", item.id)}>
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

