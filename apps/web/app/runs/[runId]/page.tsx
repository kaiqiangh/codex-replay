import { ReplayInspector } from "../../../components/replay-app";

export default async function RunDetailPage({
  params,
}: {
  params: Promise<{ runId: string }>;
}) {
  const { runId } = await params;
  return <ReplayInspector runId={runId} />;
}
