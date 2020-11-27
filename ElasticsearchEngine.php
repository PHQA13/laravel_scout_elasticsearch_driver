<?php

namespace App\Scout;

use Elasticsearch\Client as Elasticsearch;
use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;

class ElasticsearchEngine extends Engine
{
    /**
     * The Elasticsearch client.
     *
     * @var Elasticsearch
     */
    protected $elasticsearch;

    /**
     * Create a new engine instance.
     *
     * @param Elasticsearch $elasticsearch
     * @return void
     */
    public function __construct(Elasticsearch $elasticsearch)
    {
        $this->elasticsearch = $elasticsearch;
    }

    /**
     * Update the given model in the index.
     *
     * @param \Illuminate\Database\Eloquent\Collection $models
     * @return void
     */
    public function update($models)
    {
        $params['body'] = [];
        $models->each(function ($model) use (&$params) {
            $params['body'][] = [
                'update' => [
                    '_index' => $model->searchableAs(),
                    '_id'    => $model->getScoutKey(),
                ],
            ];
            $params['body'][] = [
                'doc'           => $model->toSearchableArray(),
                'doc_as_upsert' => true,
            ];
        });
        if (!empty($params['body'])) {
            $this->elasticsearch->bulk($params);
        }
    }

    /**
     * Remove the given model from the index.
     *
     * @param \Illuminate\Database\Eloquent\Collection $models
     * @return void
     */
    public function delete($models)
    {
        $params['body'] = [];
        $models->each(function ($model) use (&$params) {
            $params['body'][] = [
                'delete' => [
                    '_index' => $model->searchableAs(),
                    '_id'    => $model->getScoutKey(),
                ],
            ];
        });
        if (!empty($params['body'])) {
            $this->elasticsearch->bulk($params);
        }
    }

    /**
     * Perform the given search on the engine.
     *
     * @param \Laravel\Scout\Builder $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, [
            'filters' => $this->filters($builder),
            'from'    => 0,
            'size'    => empty($builder->limit) ? 10 : $builder->limit,
        ]);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param \Laravel\Scout\Builder $builder
     * @param int                    $perPage
     * @param int                    $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        $result = $this->performSearch($builder, [
            'filters' => $this->filters($builder),
            'from'    => (($page * $perPage) - $perPage),
            'size'    => $perPage,
        ]);
        $result['nbPages'] = $result['hits']['total']['value'] / $perPage;

        return $result;
    }

    /**
     * @param \Laravel\Scout\Builder $builder
     * @param array                  $options
     * @return array|mixed
     */
    protected function performSearch(Builder $builder, $options = [])
    {
        $params = [
            'index' => $builder->index ?: $builder->model->searchableAs(),
            'body'  => [
                'query' => [
                    //                    'match' => [
                    //                        'description' => $builder->query,
                    //                    ],
                ],
                'from'  => $options['from'],
                'size'  => $options['size'],
            ],
        ];
        if ($sort = $this->sort($builder)) {
            $params['body']['sort'] = $sort;
        }
        if (isset($options['filters']) && count($options['filters'])) {
            $params['body']['query']['bool']['filter'] = $options['filters'];
        }
        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elasticsearch,
                $builder->query,
                $params
            );
        }

        return $this->elasticsearch->search($params);
    }

    /**
     * Get the filter array for the query.
     *
     * @param \Laravel\Scout\Builder $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return collect($builder->wheres)->map(function ($value, $key) {
            return [
                'term' => [
                    $key => $value,
                ],
            ];
        })->values()->all();
    }

    protected function sort(Builder $builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }

        return collect($builder->orders)->map(function ($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param mixed $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param \Laravel\Scout\Builder              $builder
     * @param mixed                               $results
     * @param \Illuminate\Database\Eloquent\Model $model
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function map(Builder $builder, $results, $model)
    {
        if ($results['hits']['total']['value'] === 0) {
            return $model->newCollection();
        }

        $objectIds = collect($results['hits']['hits'])->pluck('_id')->values()->all();
        $objectIdPositions = array_flip($objectIds);

        return $model->getScoutModelsByIds(
            $builder, $objectIds
        )->filter(function ($model) use ($objectIds) {
            return in_array($model->getScoutKey(), $objectIds);
        })->sortBy(function ($model) use ($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        })->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param mixed $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return $results['hits']['total']['value'];
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param \Illuminate\Database\Eloquent\Model $model
     * @return void
     */
    public function flush($model)
    {
        $this->elasticsearch->indices()->delete(['index' => $model->searchableAs()]);
    }
}
