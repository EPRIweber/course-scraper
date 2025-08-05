import React from 'react';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef, GridReadyEvent } from 'ag-grid-community';

// AG Grid styles
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';

interface GridViewProps {
  rowData: Record<string, any>[];
  headers: string[];
}

export default function GridView({ rowData, headers }: GridViewProps) {
  const [gridApi, setGridApi] = React.useState<any>(null);
  const [columnApi, setColumnApi] = React.useState<any>(null);

  const onGridReady = (params: GridReadyEvent) => {
    setGridApi(params.api);
    setColumnApi(params.api.getColumn);
    params.api.sizeColumnsToFit();
  };

  // Build column definitions: row number + dynamic columns
  const columns: ColDef[] = [
    {
      headerName: '#',
      valueGetter: (params) => (params.node?.rowIndex != null ? params.node.rowIndex + 1 : ''),
      width: 60,
      pinned: 'left',
      suppressHeaderMenuButton: true,
    },
    ...headers.map((field) => ({
      headerName: field.replace(/_/g, ' '),
      field,
      sortable: true,
      filter: true,
      resizable: true,
    })),
  ];

  return (
    <div className="ag-theme-alpine" style={{ width: '100%', height: '600px' }}>
      <AgGridReact
        onGridReady={onGridReady}
        rowData={rowData}
        columnDefs={columns}
        rowSelection="multiple"
        // suppressRowClickSelection={false}
        enableRangeSelection={true}
        defaultColDef={{
          flex: 1,
          minWidth: 120,
        }}
      />
    </div>
  );
}
