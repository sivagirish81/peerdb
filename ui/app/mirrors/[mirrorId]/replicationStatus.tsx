'use client';
import { fetcher } from '@/app/utils/swr';
import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';
import {
  OplogTimestamp,
  ReplicationStatusResponse,
  ReplicationStatusState,
  replicationStatusStateFromJSON,
} from '@/grpc_generated/route';
import { Badge } from '@/lib/Badge';
import { Label } from '@/lib/Label';
import { Tooltip } from '@/lib/Tooltip';
import moment from 'moment';
import useSWR from 'swr';

type ReplicationStatusProps = {
  flowJobName: string;
  sourceType?: DBType;
};

const lagTooltip =
  "Difference between MongoDB's latest majority-committed oplog timestamp and the timestamp encoded in PeerDB's persisted resume token.";

function isMongoSource(sourceType?: DBType) {
  return (
    sourceType !== undefined && dBTypeFromJSON(sourceType) === DBType.MONGO
  );
}

function formatOplogDate(position?: OplogTimestamp) {
  if (!position) return '';
  return moment
    .unix(position.seconds)
    .utc()
    .format('MMM D, YYYY, h:mm:ss A [UTC]');
}

function formatOplogTimestamp(position?: OplogTimestamp) {
  if (!position) return '';
  return `Timestamp(${position.seconds}, ${position.increment})`;
}

function formatDuration(seconds?: number) {
  if (seconds === undefined) return '';
  seconds = Math.max(Math.floor(seconds), 0);
  if (seconds < 60) {
    return `${seconds} second${seconds === 1 ? '' : 's'}`;
  }
  if (seconds < 3600) {
    const minutes = Math.floor(seconds / 60);
    const remainder = seconds % 60;
    return remainder
      ? `${minutes} minute${minutes === 1 ? '' : 's'} ${remainder} second${
          remainder === 1 ? '' : 's'
        }`
      : `${minutes} minute${minutes === 1 ? '' : 's'}`;
  }
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return minutes
    ? `${hours} hour${hours === 1 ? '' : 's'} ${minutes} minute${
        minutes === 1 ? '' : 's'
      }`
    : `${hours} hour${hours === 1 ? '' : 's'}`;
}

function exactUtc(date?: Date | string) {
  if (!date) return '';
  return moment(date).utc().format('MMM D, YYYY, h:mm:ss A [UTC]');
}

function relative(date?: Date | string) {
  if (!date) return '';
  return moment(date).fromNow();
}

function stateLabel(state?: ReplicationStatusState) {
  switch (state) {
    case ReplicationStatusState.REPLICATION_STATUS_STATE_RUNNING:
      return 'Running';
    case ReplicationStatusState.REPLICATION_STATUS_STATE_INITIALIZING:
      return 'Initializing';
    case ReplicationStatusState.REPLICATION_STATUS_STATE_PAUSED:
      return 'Paused';
    case ReplicationStatusState.REPLICATION_STATUS_STATE_UNAVAILABLE:
      return 'Unavailable';
    default:
      return 'Unavailable';
  }
}

function SkeletonLine({ width }: { width: string }) {
  return (
    <div
      style={{
        width,
        height: 16,
        borderRadius: 4,
        background: 'rgba(128, 128, 128, 0.18)',
      }}
    />
  );
}

function Metric({
  label,
  primary,
  secondary,
  tooltip,
}: {
  label: string;
  primary?: string;
  secondary?: string;
  tooltip?: string;
}) {
  const labelNode = (
    <Label variant='subheadline' colorName='lowContrast'>
      {label}
    </Label>
  );
  return (
    <div style={{ minWidth: 0 }}>
      <div>
        {tooltip ? <Tooltip content={tooltip}>{labelNode}</Tooltip> : labelNode}
      </div>
      <div style={{ minHeight: 24 }}>
        {primary ? <Label>{primary}</Label> : <SkeletonLine width='70%' />}
      </div>
      <div style={{ minHeight: 22 }}>
        {secondary ? (
          <Label
            as='code'
            colorName='lowContrast'
            style={{ fontFamily: 'monospace', fontSize: 12 }}
          >
            {secondary}
          </Label>
        ) : (
          <SkeletonLine width='48%' />
        )}
      </div>
    </div>
  );
}

function FooterItem({
  label,
  value,
  tooltip,
}: {
  label: string;
  value?: string;
  tooltip?: string;
}) {
  return (
    <div style={{ minWidth: 0 }}>
      <Label variant='subheadline' colorName='lowContrast'>
        {label}
      </Label>{' '}
      {value ? (
        tooltip ? (
          <Tooltip content={tooltip}>
            <Label>{value}</Label>
          </Tooltip>
        ) : (
          <Label>{value}</Label>
        )
      ) : (
        <SkeletonLine width='7rem' />
      )}
    </div>
  );
}

export default function ReplicationStatus({
  flowJobName,
  sourceType,
}: ReplicationStatusProps) {
  const shouldRender = isMongoSource(sourceType);
  const { data, isLoading } = useSWR<ReplicationStatusResponse>(
    shouldRender
      ? `/api/v1/mirrors/cdc/replication-status/${encodeURIComponent(flowJobName)}`
      : null,
    fetcher,
    { refreshInterval: 10_000, revalidateOnFocus: true }
  );

  if (!shouldRender) {
    return null;
  }

  const state =
    data?.state === undefined
      ? undefined
      : replicationStatusStateFromJSON(data.state);
  const mongo = data?.mongo;
  const currentPosition = mongo?.currentPosition;
  const processedPosition = mongo?.processedPosition;
  const isInitializing =
    state === ReplicationStatusState.REPLICATION_STATUS_STATE_INITIALIZING;
  const isPaused =
    state === ReplicationStatusState.REPLICATION_STATUS_STATE_PAUSED;
  const isUnavailable =
    state === ReplicationStatusState.REPLICATION_STATUS_STATE_UNAVAILABLE;

  const checkpointUpdated = data?.checkpointUpdatedAt;
  const observedAt = data?.observedAt;

  return (
    <section
      style={{
        border: '1px solid rgba(128, 128, 128, 0.22)',
        borderRadius: 8,
        padding: '1rem',
        marginTop: '1.5rem',
        marginBottom: '1.5rem',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: '1rem',
          marginBottom: '1rem',
        }}
      >
        <Label variant='headline'>Replication Status</Label>
        {isLoading && !data ? (
          <SkeletonLine width='5rem' />
        ) : (
          <Badge type='longText'>{stateLabel(state)}</Badge>
        )}
      </div>
      <div style={{ marginBottom: '1rem' }}>
        <Label colorName='lowContrast'>
          MongoDB source {'->'} PeerDB CDC checkpoint lag
        </Label>
      </div>

      {isInitializing && (
        <div style={{ marginBottom: '1rem' }}>
          <Label>Replication status is initializing</Label>
          <br />
          <Label colorName='lowContrast'>{data?.message}</Label>
        </div>
      )}
      {isPaused && data?.message && (
        <div style={{ marginBottom: '1rem' }}>
          <Label colorName='lowContrast'>{data.message}</Label>
        </div>
      )}
      {isUnavailable && (
        <div style={{ marginBottom: '1rem' }}>
          <Label>Replication status unavailable</Label>
          <br />
          <Label colorName='lowContrast'>{data?.message}</Label>
        </div>
      )}

      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(14rem, 1fr))',
          gap: '1rem',
          marginBottom: '1rem',
        }}
      >
        <Metric
          label='Latest committed oplog'
          primary={formatOplogDate(currentPosition)}
          secondary={formatOplogTimestamp(currentPosition)}
        />
        <Metric
          label='PeerDB processed position'
          primary={formatOplogDate(processedPosition)}
          secondary={formatOplogTimestamp(processedPosition)}
        />
        <Metric
          label='Replication lag'
          primary={
            data && !isInitializing && !isUnavailable
              ? formatDuration(data.lagSeconds)
              : undefined
          }
          tooltip={lagTooltip}
        />
      </div>

      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '1rem 1.5rem',
        }}
      >
        <FooterItem
          label='Checkpoint updated'
          value={checkpointUpdated ? relative(checkpointUpdated) : undefined}
          tooltip={exactUtc(checkpointUpdated)}
        />
        <FooterItem
          label='Sync interval'
          value={
            data
              ? formatDuration(data.configuredSyncIntervalSeconds)
              : undefined
          }
        />
        <FooterItem
          label='Checked'
          value={observedAt ? relative(observedAt) : undefined}
          tooltip={exactUtc(observedAt)}
        />
      </div>
    </section>
  );
}
