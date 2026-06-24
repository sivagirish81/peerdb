'use client';
import { fetcher } from '@/app/utils/swr';
import { DBType } from '@/grpc_generated/peers';
import { CDCTableTotalCountsResponse } from '@/grpc_generated/route';
import useSWR from 'swr';
import CdcGraph from './cdcGraph';
import ReplicationStatus from './replicationStatus';
import RowsDisplay from './rowsDisplay';
import { SyncStatusTable } from './syncStatusTable';
import TableStats from './tableStats';

type SyncStatusProps = {
  flowJobName: string;
  sourceType?: DBType;
};

export default function SyncStatus({
  flowJobName,
  sourceType,
}: SyncStatusProps) {
  const {
    data: tableStats,
    error,
    isLoading,
  } = useSWR<CDCTableTotalCountsResponse>(
    `/api/v1/mirrors/cdc/table_total_counts/${encodeURIComponent(flowJobName)}`,
    fetcher
  );

  return (
    <div>
      <ReplicationStatus flowJobName={flowJobName} sourceType={sourceType} />
      {!isLoading &&
        !error &&
        tableStats &&
        tableStats?.totalData &&
        tableStats?.tablesData && (
          <>
            <RowsDisplay totalRowsData={tableStats.totalData} />
            <div className='my-10'>
              <CdcGraph mirrorName={flowJobName} />
            </div>
            <SyncStatusTable mirrorName={flowJobName} />
            <TableStats tableSyncs={tableStats.tablesData} />
          </>
        )}
    </div>
  );
}
